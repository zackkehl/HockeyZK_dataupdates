library(DBI)
library(RMySQL)
library(dplyr)
library(tidyr)
library(readr)
library(purrr)
library(stringr)
library(lubridate)
library(zoo)
library(glue)
library(jsonlite)
library(rvest)
library(reshape2)
library(data.table)
library(xgboost)
library(fastDummies)
library(Matrix)
library(glmnet)

team_abbr <- c("ANA","ARI","BOS","BUF","CGY","CAR","CHI","COL","CBJ","DAL","DET","EDM","FLA","LAK","MIN","MTL","NSH","NJD","NYI","NYR","OTT","PHI","PIT","SJS","SEA","STL","TBL","TOR","UTA","VAN","VGK","WSH","WPG")
team_names <- c("Anaheim Ducks","Arizona Coyotes","Boston Bruins","Buffalo Sabres","Calgary Flames","Carolina Hurricanes","Chicago Blackhawks","Colorado Avalanche","Columbus Blue Jackets","Dallas Stars","Detroit Red Wings","Edmonton Oilers","Florida Panthers","Los Angeles Kings","Minnesota Wild","Montreal Canadiens","Nashville Predators","New Jersey Devils","New York Islanders","New York Rangers","Ottawa Senators","Philadelphia Flyers","Pittsburgh Penguins","San Jose Sharks","Seattle Kraken","St. Louis Blues","Tampa Bay Lightning","Toronto Maple Leafs","Utah Hockey Club","Vancouver Canucks","Vegas Golden Knights","Washington Capitals","Winnipeg Jets")
teamId <- c(24,53,6,7,20,12,16,21,29,25,17,22,13,26,30,8,18,1,2,3,9,4,5,28,55,19,14,10,59,23,54,15,52)
team_table <- data.frame(team_abbr,team_names,teamId)

`%not_in%` <- Negate(`%in%`)
xg_model_5v5 <- readRDS("xg_model_5v5.rds")
xg_model_st <- readRDS("xg_model_st.rds")
xg_prep <- function(x){
  df <- x %>%
    mutate(
      event_idx = str_pad(event_idx, width = 4, side = "left", pad = 0),
      event_id = as.numeric(as.character(paste0(game_id,event_idx)))
    ) %>%
    filter(period <= 4) %>%
    filter(event_type %in% c("GOAL", "SHOT_ON_GOAL", "MISSED_SHOT")) %>%
    filter(secondary_type != "Penalty Shot" | is.na(secondary_type)) %>%
    group_by(game_id) %>%
    mutate(
      time_since_last = game_seconds - lag(game_seconds)
    ) %>%
    mutate(
      time_since_last = ifelse(is.na(time_since_last), game_seconds, time_since_last)
    ) %>%
    ungroup() %>%
    mutate(
      shot_type = secondary_type,
      rebound = ifelse(time_since_last <= 2, 1, 0),
      empty_net = ifelse(is.na(empty_net) | empty_net == FALSE, 0, 1),
      goal = ifelse(event_type == "GOAL", 1, 0)
    ) %>%
    select(season, game_id, event_id, event_idx, strength_state, shot_distance, shot_angle, rebound, empty_net, goal,shot_type) %>%
    filter(shot_type != "")
  
  df <- df %>% 
    mutate(type_value = 1) %>%
    pivot_wider(names_from = shot_type, values_from = type_value, values_fill = 0)
  
  missing_feats <- tibble(feature = xg_model_5v5$feature_names) %>%
    filter(feature %not_in% names(df)) %>%
    mutate(val = 0) %>%
    pivot_wider(names_from = feature, values_from = val)
  
  if(length(missing_feats) > 0){
    df <- bind_cols(df, missing_feats)
  }
  
  return(df)
}
calculate_xg <- function(x){
  data <- xg_prep(x)
  
  # 5v5 #
  xg_5v5 <- predict(
    xg_model_5v5,
    xgb.DMatrix(
      data = data %>% 
        filter(strength_state == "5v5") %>% 
        select(all_of(xg_model_5v5$feature_names)) %>%
        data.matrix(),
      label = data %>%
        filter(strength_state == "5v5") %>% 
        select(goal) %>%
        data.matrix()
    )
  ) %>%
    as_tibble() %>%
    rename(xg = value) %>%
    bind_cols(
      select(
        filter(data, strength_state == "5v5"),
        event_idx
      )
    )
  xg_5v5$event_idx = as.numeric(as.character(xg_5v5$event_idx))
  
  # ST #
  xg_st <- predict(
    xg_model_st,
    xgb.DMatrix(
      data = data %>% 
        filter(strength_state != "5v5") %>% 
        select(all_of(xg_model_st$feature_names)) %>%
        data.matrix(),
      label = data %>%
        filter(strength_state != "5v5") %>% 
        select(goal) %>%
        data.matrix()
    )
  ) %>%
    as_tibble() %>%
    rename(xg = value) %>%
    bind_cols(
      select(
        filter(data, strength_state != "5v5"),
        event_idx
      )
    )
  xg_st$event_idx = as.numeric(as.character(xg_st$event_idx))
  
  # Combine #
  xg_pred <- bind_rows(xg_5v5,xg_st)
  x$event_idx <- as.numeric(as.character(x$event_idx))
  xg_pred <- xg_pred %>%
    right_join(x, by = "event_idx") %>%
    arrange(event_idx)
  
  return(xg_pred)
  
}
game_scraper <- function(game_id){
  url <- glue("https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play")
  data_raw <- read_json(url)
  
  url_shifts <- glue("https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={game_id}")
  shifts_raw <- read_json(url_shifts)
  
  # Collect game id, season and game date
  game_info <- data_raw$id %>%
    tibble() %>%
    rename(game_id = ".")
  season <- data_raw$season %>%
    tibble() %>%
    rename(season = ".")
  date <- data_raw$gameDate %>%
    tibble %>%
    rename(game_date = ".")
  game_info <- bind_cols(game_info,season,date)
  
  # Collect team info
  away <- data_raw$awayTeam %>%
    unlist() %>%
    bind_rows() %>%
    rename(away_id = id, away_abbr = abbrev, away_name = "commonName.default") 
  away <- away %>%
    mutate(away_team = team_table[team_table$team_abbr == away$away_abbr, "team_names"]) %>%
    select(away_id,away_abbr,away_team)
  home <- data_raw$homeTeam %>%
    unlist() %>%
    bind_rows() %>%
    rename(home_id = id, home_abbr = abbrev, home_name = "commonName.default")
  home <- home %>%
    mutate(home_team = team_table[team_table$team_abbr == home$home_abbr, "team_names"]) %>%
    select(home_id,home_abbr,home_team)
  teams <- bind_cols(away,home)
  
  # Team rosters
  rosters <- data_raw$rosterSpots %>%
    tibble() %>%
    unnest_wider(1) %>%
    unnest_wider(firstName) %>%
    rename(first_name = default) %>%
    unnest_wider(lastName,names_sep="_") %>%
    rename(last_name = lastName_default) %>%
    mutate(full_name = paste(first_name,last_name,sep=" "),
           position = case_when(
             positionCode %in% c("C","L","R") ~ "F",
             positionCode == "D" ~ "D",
             positionCode == "G" ~ "G"
           )) %>%
    arrange(teamId,factor(positionCode,levels=c("C","L","R","D","G"))) %>%
    select(teamId,playerId,full_name,last_name,first_name,positionCode,position)
  
  # Collect play data
  plays <- data_raw$plays %>%
    tibble() %>%
    unnest_wider(1) %>%
    unnest_wider(periodDescriptor, names_sep = "_") %>%
    unnest_wider(details,names_sep = "_") %>%
    rename(period = periodDescriptor_number)
  
  # Combine game info, play data, team data
  plays <- bind_cols(game_info,plays,teams)
  
  # Add potential missing columns
  int_columns <- c(
    "details_eventOwnerTeamId", "period", "details_homeScore", "details_awayScore",
    "details_scoringPlayerId", "details_shootingPlayerId", "details_hittingPlayerId", "details_winningPlayerId",
    "details_committedByPlayerId", "details_playerId", "details_assist1PlayerId", "details_blockingPlayerId",
    "details_goalieInNetId", "details_hitteePlayerId", "details_losingPlayerId", "details_drawnByPlayerId",
    "details_assist2PlayerId", "details_goalieInNetId", "details_servedByPlayerId",
    "details_xCoord", "details_yCoord"
  )
  
  char_columns <- c(
    "typeDescKey", "details_shotType", "details_descKey", "timeInPeriod", "timeRemaining",
    "homeTeamDefendingSide", "typeCode", "situationCode"
  )
  
  for(i in int_columns){
    if(i %not_in% names(plays)){
      plays[, i] <- NA_integer_
    }
  }
  
  for(i in char_columns){
    if(i %not_in% names(plays)){
      plays[, i] <- NA_character_
    }
  }
  
  # Reformat time, add period time (and time remaining), game time (and time remaining)
  plays <- plays %>%
    mutate(
      period_seconds = period_to_seconds(ms(timeInPeriod)),
      period_seconds_remaining = 1200 - period_seconds,
      game_seconds = ((period-1)*1200) + period_seconds,
      game_seconds_remaining = 3600 - game_seconds
    )
  
  # Add event details
  plays <- plays %>%
    mutate(
      event = str_to_title((gsub("-"," ",typeDescKey))),
      event_type = toupper(gsub(" ","_",event)),
      secondary_type = case_when(
        event %in% c("Goal","Shot On Goal","Blocked Shot","Missed Shot") ~ details_shotType,
        event == "Penalty" ~ details_descKey
      ),
      event_team = details_eventOwnerTeamId,
      event_team_type = ifelse(event_team == away_id,"away","home"),
      event_team_id = ifelse(event_team == away_id,away_id,home_id),
      event_team_name = ifelse(event_team == away_id,away_team,home_team),
      event_player_1_id = case_when(
        event %in% c("Blocked Shot","Missed Shot","Shot On Goal") ~ details_shootingPlayerId,
        event == "Faceoff" ~ details_winningPlayerId,
        event %in% c("Giveaway","Takeaway") ~ details_playerId,
        event == "Goal" ~ details_scoringPlayerId,
        event == "Hit" ~ details_hittingPlayerId,
        event == "Penalty" ~ details_committedByPlayerId
      ),
      event_player_1_type = case_when(
        event %in% c("Blocked Shot","Missed Shot","Shot On Goal") ~ "Shooter",
        event == "Faceoff" ~ "Winner",
        event == "Goal" ~ "Scorer",
        event == "Hit" ~ "Hitter",
        event == "Penalty" ~ "PenaltyOn"
      ),
      event_player_2_id = case_when(
        event == "Blocked Shot" ~ details_blockingPlayerId,
        event == "Faceoff" ~ details_losingPlayerId,
        event == "Goal" ~ details_assist1PlayerId,
        event == "Hit" ~ details_hitteePlayerId,
        event == "Penalty" ~ details_drawnByPlayerId
      ),
      event_player_2_type = case_when(
        event == "Blocked Shot" ~ "Blocker",
        event == "Faceoff" ~ "Loser",
        event == "Goal" ~ "Assist",
        event == "Hit" ~ "Hittee",
        event == "Penalty" ~ "DrewBy"
      ),
      event_player_3_id = case_when(
        event == "Goal" ~ details_assist2PlayerId,
        event == "Penalty" ~ details_servedByPlayerId
      ),
      event_player_3_type = case_when(
        event == "Goal" ~ "Assist",
        event == "Penalty" ~ "ServedBy"
      ),
      event_goalie_id = details_goalieInNetId,
      home_skaters = as.integer(substr(situationCode, 3, 3)),
      away_skaters = as.integer(substr(situationCode, 2, 2)),
      home_goalie_in = as.integer(substr(situationCode, 4, 4)),
      away_goalie_in = as.integer(substr(situationCode, 1, 1)),
      extra_attacker = case_when(
        event_team_id == home_id & home_goalie_in == 0 ~ TRUE,
        event_team_id == away_id & away_goalie_in == 0 ~ TRUE,
        TRUE ~ FALSE
      ),
      empty_net = case_when(
        event_team_id == home_id & away_goalie_in == 0 & event %in% c("Blocked Shot","Missed Shot","Shot On Goal","Goal") ~ TRUE,
        event_team_id == away_id & home_goalie_in == 0 & event %in% c("Blocked Shot","Missed Shot","Shot On Goal","Goal") ~ TRUE,
        TRUE ~ FALSE
      ),
      strength_state = case_when(
        event_team_type == "home" ~ glue("{home_skaters}v{away_skaters}"),
        event_team_type == "away" ~ glue("{away_skaters}v{home_skaters}"),
        TRUE ~ glue("{home_skaters}v{away_skaters}")
      ),
      # change x coordinates so that home team always shoots to the right
      x = case_when(
        event_team_type == "home" & homeTeamDefendingSide == "right" ~ 0 - details_xCoord,
        event_team_type == "away" & homeTeamDefendingSide == "right" ~ 0 - details_xCoord,
        TRUE ~ details_xCoord
      ),
      y = case_when(
        event_team_type == "home" & homeTeamDefendingSide == "right" ~ 0 - details_yCoord,
        event_team_type == "away" & homeTeamDefendingSide == "right" ~ 0 - details_yCoord,
        TRUE ~ details_yCoord
      ),
      # add shot distance/angle
      shot_distance = case_when(
        event_team_type == "home" & event %in% c("Goal","Missed Shot","Shot On Goal") ~
          round(abs(sqrt((x - 89)^2 + (y)^2)),1),
        event_team_type == "away" & event %in% c("Goal","Missed Shot","Shot On Goal") ~
          round(abs(sqrt((x - (-89))^2 + (y)^2)),1),
        TRUE ~ NA_real_
      ),
      shot_angle = case_when(
        event_team_type == "home" & event %in% c("Goal","Missed Shot","Shot On Goal") ~
          round(abs(atan((0-y) / (89-x)) * (180 / pi)),1),
        event_team_type == "away" & event %in% c("Goal","Missed Shot","Shot On Goal") ~
          round(abs(atan((0-y) / (-89-x)) * (180 / pi)),1),
        TRUE ~ NA_real_
      ),
      # fix behind the net angles
      shot_angle = ifelse(
        (event_team_type == "home" & x > 89) |
          (event_team_type == "away" & x < -89),
        180 - shot_angle,
        shot_angle
      ),
      home_final = last(details_homeScore,na_rm = TRUE),
      away_final = last(details_awayScore,na_rm = TRUE)
    ) %>%
    select(
      season,game_date,game_id,event,event_type,secondary_type,period,period_seconds,period_seconds_remaining,game_seconds,game_seconds_remaining,
      event_team_name,event_team_type,event_team_id,event_player_1_id,event_player_1_type,event_player_2_id,event_player_2_type,
      event_player_3_id,event_player_3_type,event_goalie_id,
      home_score = details_homeScore,away_score = details_awayScore,home_final,away_final,
      strength_state,empty_net,extra_attacker,home_skaters,away_skaters,
      x,y,shot_distance,shot_angle,zone_code=details_zoneCode,
      away_team,away_id,away_abbr,home_team,home_id,home_abbr
    )
  
  # Shift data
  shifts <- shifts_raw$data %>%
    tibble() %>%
    unnest_wider(1) %>%
    filter(is.na(eventDescription)) %>%
    mutate(
      duration = period_to_seconds(ms(duration)),
      shift_start_game_seconds = period_to_seconds(ms(startTime)) + ((period-1)*1200),
      shift_end_game_seconds = period_to_seconds(ms(endTime)) + ((period-1)*1200),
      shift_start_period_seconds = period_to_seconds(ms(startTime)),
      shift_end_period_seconds = period_to_seconds(ms(endTime))
    )
  shifts$teamName <- team_table$team_names[match(shifts$teamAbbrev,team_table$team_abbr)]
  shifts <- shifts %>%
    left_join(rosters,by="playerId") %>%
    select(gameId,playerId,full_name,firstName,lastName,teamName,teamAbbrev,teamId=teamId.x,position,positionCode,
           shiftNumber,period,startTime,endTime,shift_start_period_seconds,shift_end_period_seconds,shift_start_game_seconds,shift_end_game_seconds,duration) %>%
    arrange(teamId,factor(positionCode,levels=c("C","L","R","D","G")),playerId)
  
  shifts_on <- shifts %>%
    group_by(teamName,period,startTime,shift_start_period_seconds,shift_start_game_seconds) %>%
    summarize(
      num_on = n(),
      players_on = paste(playerId, collapse = ", "),
      .groups = "drop"
    ) %>%
    rename(
      game_seconds = shift_start_game_seconds,
      period_time = startTime
    )
  
  shifts_off <- shifts %>%
    group_by(teamName,period,endTime,shift_end_period_seconds,shift_end_game_seconds) %>%
    summarize(
      num_off = n(),
      players_off = paste(playerId, collapse = ", "),
      .groups = "drop"
    ) %>%
    rename(
      game_seconds = shift_end_game_seconds,
      period_time = endTime
    )
  
  shifts <- full_join(
    shifts_on,shifts_off,
    by=c("game_seconds","teamName","period","period_time")
  ) %>%
    arrange(game_seconds) %>%
    mutate(
      event = "Change",
      event_type = "CHANGE",
      game_seconds_remaining = 3600 - game_seconds
    ) %>%
    mutate(
      players_on = ifelse(is.na(players_on), "None", players_on),
      players_off = ifelse(is.na(players_off), "None", players_off),
    ) %>%
    rename(event_team_name = teamName)
  
  # Add player changes into plays data
  pbp <- bind_rows(plays,shifts) %>%
    mutate(
      priority =
        1 * (event_type %in% c("TAKEAWAY", "GIVEAWAY", "MISSED_SHOT", "HIT", "SHOT_ON_GOAL", "BLOCKED_SHOT") & period !=5) +
        2 * (event_type == "GOAL" & period !=5) +
        3 * (event_type == "STOPPAGE" & period !=5) +
        4 * (event_type == "PENALTY" & period !=5) +
        5 * (event_type == "CHANGE" & period !=5) +
        6 * (event_type == "PERIOD_END" & period !=5) +
        7 * (event_type == "GAME_END" & period !=5) +
        8 * (event_type == "FACEOFF" &  period !=5)
    ) %>%
    arrange(period,game_seconds,priority) %>%
    mutate(
      home_index = as.numeric(cumsum(event_type == "CHANGE" &
                                       event_team_name == unique(plays$home_team))),
      away_index = as.numeric(cumsum(event_type == "CHANGE" &
                                       event_team_name == unique(plays$away_team)))
    ) %>%
    select(-priority)
  
  rosters <- rosters %>% filter(position != "G")
  
  home_skaters <- NULL
  
  for(i in 1:nrow(rosters)){
    
    player <- as.character(rosters$playerId[i])
    
    skaters_i <- tibble(
      on_ice = cumsum(
        1 * str_detect(
          filter(pbp,event_type == "CHANGE" &
                   event_team_name %in% unique(pbp$home_team))$players_on,
          player) -
          1 * str_detect(
            filter(pbp,event_type == "CHANGE" &
                     event_team_name %in% unique(pbp$home_team))$players_off,
            player)
      )
    )
    
    suppressMessages({home_skaters <- bind_cols(home_skaters, skaters_i)})
    rm(skaters_i, player)
  }
  
  colnames(home_skaters) <- rosters$playerId
  
  home_skaters <- data.frame(home_skaters)
  
  on_home <- which(home_skaters == 1, arr.ind = TRUE) %>%
    data.frame() %>%
    group_by(row) %>%
    summarize(
      home_on_1 = colnames(home_skaters)[unique(col)[1]],
      home_on_2 = colnames(home_skaters)[unique(col)[2]],
      home_on_3 = colnames(home_skaters)[unique(col)[3]],
      home_on_4 = colnames(home_skaters)[unique(col)[4]],
      home_on_5 = colnames(home_skaters)[unique(col)[5]],
      home_on_6 = colnames(home_skaters)[unique(col)[6]],
      home_on_7 = colnames(home_skaters)[unique(col)[7]]
    ) %>%
    mutate(
      across(
        .cols = home_on_1:home_on_7,
        ~as.integer(gsub("X","",.x))
      )
    )
  
  away_skaters <- NULL
  
  for(i in 1:nrow(rosters)){
    
    player <- as.character(rosters$playerId[i])
    
    skaters_i <- tibble(
      on_ice = cumsum(
        1 * str_detect(
          filter(pbp,event_type == "CHANGE" &
                   event_team_name %in% unique(pbp$away_team))$players_on,
          player) -
          1 * str_detect(
            filter(pbp,event_type == "CHANGE" &
                     event_team_name %in% unique(pbp$away_team))$players_off,
            player)
      )
    )
    
    suppressMessages({away_skaters <- bind_cols(away_skaters, skaters_i)})
    rm(skaters_i, player)
  }
  
  colnames(away_skaters) <- rosters$playerId
  
  away_skaters <- data.frame(away_skaters)
  
  on_away <- which(away_skaters == 1, arr.ind = TRUE) %>%
    data.frame() %>%
    group_by(row) %>%
    summarize(
      away_on_1 = colnames(away_skaters)[unique(col)[1]],
      away_on_2 = colnames(away_skaters)[unique(col)[2]],
      away_on_3 = colnames(away_skaters)[unique(col)[3]],
      away_on_4 = colnames(away_skaters)[unique(col)[4]],
      away_on_5 = colnames(away_skaters)[unique(col)[5]],
      away_on_6 = colnames(away_skaters)[unique(col)[6]],
      away_on_7 = colnames(away_skaters)[unique(col)[7]]
    ) %>%
    mutate(
      across(
        .cols = away_on_1:away_on_7,
        ~as.integer(gsub("X","",.x))
      )
    )
  
  # Add on ice players to pbp
  pbp_full <- pbp %>%
    left_join(on_home, by = c("home_index" = "row")) %>%
    left_join(on_away, by = c("away_index" = "row"))
  
  # Select needed columns
  pbp_full <- pbp_full %>%
    mutate(season = pbp_full$season[1],
           game_date = pbp_full$game_date[1],
           game_id = pbp_full$game_id[1]) %>%
    #    filter(event != "Change") %>%
    mutate(event_id = str_pad(row_number(),width=4,side="left",pad=0),
           event_idx = as.numeric(paste0(game_id,event_id))) %>%
    select(season,game_date,game_id,event_idx,event_id,event,event_type,secondary_type,
           period,period_seconds,period_seconds_remaining,game_seconds,game_seconds_remaining,
           event_team_name,event_team_type,event_team_id,
           event_player_1_id,event_player_1_type,event_player_2_id,event_player_2_type,event_player_3_id,event_player_3_type,event_goalie_id,
           home_score,home_final,away_score,away_final,
           strength_state,empty_net,extra_attacker,home_skaters,away_skaters,
           home_on_1,home_on_2,home_on_3,home_on_4,home_on_5,home_on_6,home_on_7,
           away_on_1,away_on_2,away_on_3,away_on_4,away_on_5,away_on_6,away_on_7,
           x,y,shot_distance,shot_angle,zone_code,
           away_team,away_abbr,away_id,home_team,home_abbr,home_id)
  
  pbp_full$home_score[1] <- 0
  pbp_full$away_score[1] <- 0
  pbp_full$home_score <- zoo::na.locf(pbp_full$home_score)
  pbp_full$away_score <- zoo::na.locf(pbp_full$away_score)
  pbp_full$home_final <- last(pbp_full$home_score,na.rm = TRUE)
  pbp_full$away_final <- last(pbp_full$away_score,na.rm = TRUE)
  
  # Add xg
  pbp_full <- calculate_xg(pbp_full)
  
  return(pbp_full)
}
game_scraper_html <- function(game_id){
  url <- glue("https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play")
  data_raw <- read_json(url)
  
  # Collect game id, season and game date
  game_info <- data_raw$id %>%
    tibble() %>%
    rename(game_id = ".")
  season <- data_raw$season %>%
    tibble() %>%
    rename(season = ".")
  date <- data_raw$gameDate %>%
    tibble %>%
    rename(game_date = ".")
  game_info <- bind_cols(game_info,season,date)
  
  # Collect team info
  away <- data_raw$awayTeam %>%
    unlist() %>%
    bind_rows() %>%
    rename(away_id = id, away_abbr = abbrev, away_name = "commonName.default") 
  away <- away %>%
    mutate(away_team = team_table[team_table$team_abbr == away$away_abbr, "team_names"]) %>%
    select(away_id,away_abbr,away_team)
  home <- data_raw$homeTeam %>%
    unlist() %>%
    bind_rows() %>%
    rename(home_id = id, home_abbr = abbrev, home_name = "commonName.default")
  home <- home %>%
    mutate(home_team = team_table[team_table$team_abbr == home$home_abbr, "team_names"]) %>%
    select(home_id,home_abbr,home_team)
  teams <- bind_cols(away,home)
  
  # Team rosters
  rosters <- data_raw$rosterSpots %>%
    tibble() %>%
    unnest_wider(1) %>%
    unnest_wider(firstName) %>%
    rename(first_name = default) %>%
    mutate(first_name = str_to_title(first_name)) %>%
    unnest_wider(lastName,names_sep="_") %>%
    rename(last_name = lastName_default) %>%
    mutate(last_name = str_to_title(last_name)) %>%
    mutate(full_name = paste(first_name,last_name,sep=" "),
           position = case_when(
             positionCode %in% c("C","L","R") ~ "F",
             positionCode == "D" ~ "D",
             positionCode == "G" ~ "G"
           )) %>%
    arrange(teamId,factor(positionCode,levels=c("C","L","R","D","G"))) %>%
    select(teamId,playerId,full_name,last_name,first_name,positionCode,position,sweaterNumber)
  
  #rosters$full_name[rosters$playerId == 8479944] <- "Zach Aston-Reese"
  #rosters$last_name[rosters$playerId == 8479944] <- "Aston-Reese"
  #rosters$first_name[rosters$playerId == 8479944] <- "Zach"
  
  
  # Collect play data
  plays <- data_raw$plays %>%
    tibble() %>%
    unnest_wider(1) %>%
    unnest_wider(periodDescriptor, names_sep = "_") %>%
    unnest_wider(details,names_sep = "_") %>%
    rename(period = periodDescriptor_number)
  
  # Combine game info, play data, team data
  plays <- bind_cols(game_info,plays,teams)
  
  # Add potential missing columns
  int_columns <- c(
    "details_eventOwnerTeamId", "period", "details_homeScore", "details_awayScore",
    "details_scoringPlayerId", "details_shootingPlayerId", "details_hittingPlayerId", "details_winningPlayerId",
    "details_committedByPlayerId", "details_playerId", "details_assist1PlayerId", "details_blockingPlayerId",
    "details_goalieInNetId", "details_hitteePlayerId", "details_losingPlayerId", "details_drawnByPlayerId",
    "details_assist2PlayerId", "details_goalieInNetId", "details_servedByPlayerId",
    "details_xCoord", "details_yCoord"
  )
  
  char_columns <- c(
    "typeDescKey", "details_shotType", "details_descKey", "timeInPeriod", "timeRemaining",
    "homeTeamDefendingSide", "typeCode", "situationCode"
  )
  
  for(i in int_columns){
    if(i %not_in% names(plays)){
      plays[, i] <- NA_integer_
    }
  }
  
  for(i in char_columns){
    if(i %not_in% names(plays)){
      plays[, i] <- NA_character_
    }
  }
  
  # Reformat time, add period time (and time remaining), game time (and time remaining)
  plays <- plays %>%
    mutate(
      period_seconds = period_to_seconds(ms(timeInPeriod)),
      period_seconds_remaining = 1200 - period_seconds,
      game_seconds = ((period-1)*1200) + period_seconds,
      game_seconds_remaining = 3600 - game_seconds
    )
  
  # Add event details
  plays <- plays %>%
    mutate(
      event = str_to_title((gsub("-"," ",typeDescKey))),
      event_type = toupper(gsub(" ","_",event)),
      secondary_type = case_when(
        event %in% c("Goal","Shot On Goal","Blocked Shot","Missed Shot") ~ details_shotType,
        event == "Penalty" ~ details_descKey
      ),
      event_team = details_eventOwnerTeamId,
      event_team_type = ifelse(event_team == away_id,"away","home"),
      event_team_id = ifelse(event_team == away_id,away_id,home_id),
      event_team_name = ifelse(event_team == away_id,away_team,home_team),
      event_player_1_id = case_when(
        event %in% c("Blocked Shot","Missed Shot","Shot On Goal") ~ details_shootingPlayerId,
        event == "Faceoff" ~ details_winningPlayerId,
        event %in% c("Giveaway","Takeaway") ~ details_playerId,
        event == "Goal" ~ details_scoringPlayerId,
        event == "Hit" ~ details_hittingPlayerId,
        event == "Penalty" ~ details_committedByPlayerId
      ),
      event_player_1_type = case_when(
        event %in% c("Blocked Shot","Missed Shot","Shot On Goal") ~ "Shooter",
        event == "Faceoff" ~ "Winner",
        event == "Goal" ~ "Scorer",
        event == "Hit" ~ "Hitter",
        event == "Penalty" ~ "PenaltyOn"
      ),
      event_player_2_id = case_when(
        event == "Blocked Shot" ~ details_blockingPlayerId,
        event == "Faceoff" ~ details_losingPlayerId,
        event == "Goal" ~ details_assist1PlayerId,
        event == "Hit" ~ details_hitteePlayerId,
        event == "Penalty" ~ details_drawnByPlayerId
      ),
      event_player_2_type = case_when(
        event == "Blocked Shot" ~ "Blocker",
        event == "Faceoff" ~ "Loser",
        event == "Goal" ~ "Assist",
        event == "Hit" ~ "Hittee",
        event == "Penalty" ~ "DrewBy"
      ),
      event_player_3_id = case_when(
        event == "Goal" ~ details_assist2PlayerId,
        event == "Penalty" ~ details_servedByPlayerId
      ),
      event_player_3_type = case_when(
        event == "Goal" ~ "Assist",
        event == "Penalty" ~ "ServedBy"
      ),
      event_goalie_id = details_goalieInNetId,
      home_skaters = as.integer(substr(situationCode, 3, 3)),
      away_skaters = as.integer(substr(situationCode, 2, 2)),
      home_goalie_in = as.integer(substr(situationCode, 4, 4)),
      away_goalie_in = as.integer(substr(situationCode, 1, 1)),
      extra_attacker = case_when(
        event_team_id == home_id & home_goalie_in == 0 ~ TRUE,
        event_team_id == away_id & away_goalie_in == 0 ~ TRUE,
        TRUE ~ FALSE
      ),
      empty_net = case_when(
        event_team_id == home_id & away_goalie_in == 0 & event %in% c("Blocked Shot","Missed Shot","Shot On Goal","Goal") ~ TRUE,
        event_team_id == away_id & home_goalie_in == 0 & event %in% c("Blocked Shot","Missed Shot","Shot On Goal","Goal") ~ TRUE,
        TRUE ~ FALSE
      ),
      strength_state = case_when(
        event_team_type == "home" ~ glue("{home_skaters}v{away_skaters}"),
        event_team_type == "away" ~ glue("{away_skaters}v{home_skaters}"),
        TRUE ~ glue("{home_skaters}v{away_skaters}")
      ),
      # change x coordinates so that home team always shoots to the right
      x = case_when(
        event_team_type == "home" & homeTeamDefendingSide == "right" ~ 0 - details_xCoord,
        event_team_type == "away" & homeTeamDefendingSide == "right" ~ 0 - details_xCoord,
        TRUE ~ details_xCoord
      ),
      y = case_when(
        event_team_type == "home" & homeTeamDefendingSide == "right" ~ 0 - details_yCoord,
        event_team_type == "away" & homeTeamDefendingSide == "right" ~ 0 - details_yCoord,
        TRUE ~ details_yCoord
      ),
      # add shot distance/angle
      shot_distance = case_when(
        event_team_type == "home" & event %in% c("Goal","Missed Shot","Shot On Goal") ~
          round(abs(sqrt((x - 89)^2 + (y)^2)),1),
        event_team_type == "away" & event %in% c("Goal","Missed Shot","Shot On Goal") ~
          round(abs(sqrt((x - (-89))^2 + (y)^2)),1),
        TRUE ~ NA_real_
      ),
      shot_angle = case_when(
        event_team_type == "home" & event %in% c("Goal","Missed Shot","Shot On Goal") ~
          round(abs(atan((0-y) / (89-x)) * (180 / pi)),1),
        event_team_type == "away" & event %in% c("Goal","Missed Shot","Shot On Goal") ~
          round(abs(atan((0-y) / (-89-x)) * (180 / pi)),1),
        TRUE ~ NA_real_
      ),
      # fix behind the net angles
      shot_angle = ifelse(
        (event_team_type == "home" & x > 89) |
          (event_team_type == "away" & x < -89),
        180 - shot_angle,
        shot_angle
      ),
      home_final = last(details_homeScore,na_rm = TRUE),
      away_final = last(details_awayScore,na_rm = TRUE)
    ) %>%
    select(
      season,game_date,game_id,event,event_type,secondary_type,period,period_seconds,period_seconds_remaining,game_seconds,game_seconds_remaining,
      event_team_name,event_team_type,event_team_id,event_player_1_id,event_player_1_type,event_player_2_id,event_player_2_type,
      event_player_3_id,event_player_3_type,event_goalie_id,
      home_score = details_homeScore,away_score = details_awayScore,home_final,away_final,
      strength_state,empty_net,extra_attacker,home_skaters,away_skaters,
      x,y,shot_distance,shot_angle,zone_code=details_zoneCode,
      away_team,away_id,away_abbr,home_team,home_id,home_abbr
    )
  
  # Shift data
  season <- paste(as.numeric(substr(game_id,1,4)),as.numeric(substr(game_id,1,4))+1,sep = "")
  game_code <- substr(game_id,5,10)
  url <- glue("https://www.nhl.com/scores/htmlreports/{season}/TH{game_code}.HTM")
  shift_data_read <- read_html(url) %>%
    html_element("body") %>%
    html_table()
  shift_data <- shift_data_read %>%
    rename(
      shift = "X1",
      period = "X2",
      start = "X3",
      end = "X4",
      duration = "X5",
      player = "X7"
    ) %>%
    select(shift,period,start,end,duration,player) %>%
    fill(player) %>%
    tail(-23)
  to_remove <- shift_data %>%
    filter(1 == cumsum((grepl("SHF",shift_data$shift,fixed=TRUE)) - 
                         lag(shift == "TOT", default = 0)))
  shift_data <- setdiff(shift_data,to_remove) 
  shift_data <- shift_data %>%
    mutate(
      tag = ifelse((shift_data$shift == "") | (shift_data$shift == shift_data$player) | (shift_data$shift == "Shift #" | (grepl("Copyright",shift_data$shift,fixed=TRUE))),1,0),
      period = ifelse(shift_data$period == "OT",4,shift_data$period)
    ) %>%
    filter(tag != 1) %>%
    select(-tag) %>%
    separate(col = start,into = c("startTime","start_DELETE"),sep = " / ") %>%
    separate(col = end,into = c("endTime","end_DELETE"),sep = " / ")
  player_number <- colsplit(shift_data$player, " ",c("sweaterNumber","player_name"))
  shift_data_final_home <- bind_cols(shift_data,player_number) %>%
    separate(col = player_name,into = c("last_name","first_name"),sep = ", ") %>%
    mutate_at(vars(shift,period),~as.numeric(as.character(.))) %>%
    mutate(gameId = game_id,
           last_name = str_to_title(last_name),
           first_name = str_to_title(first_name),
           duration = period_to_seconds(ms(duration)),
           shift_start_game_seconds = period_to_seconds(ms(startTime)) + ((period-1)*1200),
           shift_end_game_seconds = period_to_seconds(ms(endTime)) + ((period-1)*1200),
           shift_start_period_seconds = period_to_seconds(ms(startTime)),
           shift_end_period_seconds = period_to_seconds(ms(endTime))) %>%
    left_join(rosters,by=c("first_name","last_name","sweaterNumber")) %>%
    left_join(team_table, by="teamId") %>%
    select(gameId,playerId,full_name,firstName=first_name,lastName=last_name,teamName=team_names,teamAbbrev=team_abbr,teamId,position,positionCode,
           shiftNumber=shift,period,startTime,endTime,shift_start_period_seconds,shift_end_period_seconds,
           shift_start_game_seconds,shift_end_game_seconds,duration) %>%
    arrange(teamId,factor(positionCode,levels=c("C","L","R","D","G")),playerId)
  
  url <- glue("https://www.nhl.com/scores/htmlreports/{season}/TV{game_code}.HTM")
  shift_data_read <- read_html(url) %>%
    html_element("body") %>%
    html_table()
  shift_data <- shift_data_read %>%
    rename(
      shift = "X1",
      period = "X2",
      start = "X3",
      end = "X4",
      duration = "X5",
      player = "X7"
    ) %>%
    select(shift,period,start,end,duration,player) %>%
    fill(player) %>%
    tail(-23)
  to_remove <- shift_data %>%
    filter(1 == cumsum((grepl("SHF",shift_data$shift,fixed=TRUE)) - 
                         lag(shift == "TOT", default = 0)))
  shift_data <- setdiff(shift_data,to_remove) 
  shift_data <- shift_data %>%
    mutate(
      tag = ifelse((shift_data$shift == "") | (shift_data$shift == shift_data$player) | (shift_data$shift == "Shift #" | (grepl("Copyright",shift_data$shift,fixed=TRUE))),1,0),
      period = ifelse(shift_data$period == "OT",4,shift_data$period)
    ) %>%
    filter(tag != 1) %>%
    select(-tag) %>%
    separate(col = start,into = c("startTime","start_DELETE"),sep = " / ") %>%
    separate(col = end,into = c("endTime","end_DELETE"),sep = " / ")
  player_number <- colsplit(shift_data$player, " ",c("sweaterNumber","player_name"))
  shift_data_final_away <- bind_cols(shift_data,player_number) %>%
    separate(col = player_name,into = c("last_name","first_name"),sep = ", ") %>%
    mutate_at(vars(shift,period),~as.numeric(as.character(.))) %>%
    mutate(gameId = game_id,
           last_name = str_to_title(last_name),
           first_name = str_to_title(first_name),
           duration = period_to_seconds(ms(duration)),
           shift_start_game_seconds = period_to_seconds(ms(startTime)) + ((period-1)*1200),
           shift_end_game_seconds = period_to_seconds(ms(endTime)) + ((period-1)*1200),
           shift_start_period_seconds = period_to_seconds(ms(startTime)),
           shift_end_period_seconds = period_to_seconds(ms(endTime))) %>%
    left_join(rosters,by=c("first_name","last_name","sweaterNumber")) %>%
    left_join(team_table, by="teamId") %>%
    select(gameId,playerId,full_name,firstName=first_name,lastName=last_name,teamName=team_names,teamAbbrev=team_abbr,teamId,position,positionCode,
           shiftNumber=shift,period,startTime,endTime,shift_start_period_seconds,shift_end_period_seconds,
           shift_start_game_seconds,shift_end_game_seconds,duration) %>%
    arrange(teamId,factor(positionCode,levels=c("C","L","R","D","G")),playerId)
  
  
  
  shifts <- bind_rows(shift_data_final_home,shift_data_final_away)
  
  shifts_on <- shifts %>%
    group_by(teamName,period,startTime,shift_start_period_seconds,shift_start_game_seconds) %>%
    summarize(
      num_on = n(),
      players_on = paste(playerId, collapse = ", "),
      .groups = "drop"
    ) %>%
    rename(
      game_seconds = shift_start_game_seconds,
      period_time = startTime
    )
  
  shifts_off <- shifts %>%
    group_by(teamName,period,endTime,shift_end_period_seconds,shift_end_game_seconds) %>%
    summarize(
      num_off = n(),
      players_off = paste(playerId, collapse = ", "),
      .groups = "drop"
    ) %>%
    rename(
      game_seconds = shift_end_game_seconds,
      period_time = endTime
    )
  
  shifts <- full_join(
    shifts_on,shifts_off,
    by=c("game_seconds","teamName","period","period_time")
  ) %>%
    arrange(game_seconds) %>%
    mutate(
      event = "Change",
      event_type = "CHANGE",
      game_seconds_remaining = 3600 - game_seconds
    ) %>%
    mutate(
      players_on = ifelse(is.na(players_on), "None", players_on),
      players_off = ifelse(is.na(players_off), "None", players_off),
    ) %>%
    rename(event_team_name = teamName)
  
  # Add player changes into plays data
  pbp <- bind_rows(plays,shifts) %>%
    mutate(
      priority =
        1 * (event_type %in% c("TAKEAWAY", "GIVEAWAY", "MISSED_SHOT", "HIT", "SHOT_ON_GOAL", "BLOCKED_SHOT") & period !=5) +
        2 * (event_type == "GOAL" & period !=5) +
        3 * (event_type == "STOPPAGE" & period !=5) +
        4 * (event_type == "PENALTY" & period !=5) +
        5 * (event_type == "CHANGE" & period !=5) +
        6 * (event_type == "PERIOD_END" & period !=5) +
        7 * (event_type == "GAME_END" & period !=5) +
        8 * (event_type == "FACEOFF" &  period !=5)
    ) %>%
    arrange(period,game_seconds,priority) %>%
    mutate(
      home_index = as.numeric(cumsum(event_type == "CHANGE" &
                                       event_team_name == unique(plays$home_team))),
      away_index = as.numeric(cumsum(event_type == "CHANGE" &
                                       event_team_name == unique(plays$away_team)))
    ) %>%
    select(-priority)
  
  rosters <- rosters %>% filter(position != "G")
  
  home_skaters <- NULL
  
  for(i in 1:nrow(rosters)){
    
    player <- as.character(rosters$playerId[i])
    
    skaters_i <- tibble(
      on_ice = cumsum(
        1 * str_detect(
          filter(pbp,event_type == "CHANGE" &
                   event_team_name %in% unique(pbp$home_team))$players_on,
          player) -
          1 * str_detect(
            filter(pbp,event_type == "CHANGE" &
                     event_team_name %in% unique(pbp$home_team))$players_off,
            player)
      )
    )
    
    suppressMessages({home_skaters <- bind_cols(home_skaters, skaters_i)})
    rm(skaters_i, player)
  }
  
  colnames(home_skaters) <- rosters$playerId
  
  home_skaters <- data.frame(home_skaters)
  
  on_home <- which(home_skaters == 1, arr.ind = TRUE) %>%
    data.frame() %>%
    group_by(row) %>%
    summarize(
      home_on_1 = colnames(home_skaters)[unique(col)[1]],
      home_on_2 = colnames(home_skaters)[unique(col)[2]],
      home_on_3 = colnames(home_skaters)[unique(col)[3]],
      home_on_4 = colnames(home_skaters)[unique(col)[4]],
      home_on_5 = colnames(home_skaters)[unique(col)[5]],
      home_on_6 = colnames(home_skaters)[unique(col)[6]],
      home_on_7 = colnames(home_skaters)[unique(col)[7]]
    ) %>%
    mutate(
      across(
        .cols = home_on_1:home_on_7,
        ~as.integer(gsub("X","",.x))
      )
    )
  
  away_skaters <- NULL
  
  for(i in 1:nrow(rosters)){
    
    player <- as.character(rosters$playerId[i])
    
    skaters_i <- tibble(
      on_ice = cumsum(
        1 * str_detect(
          filter(pbp,event_type == "CHANGE" &
                   event_team_name %in% unique(pbp$away_team))$players_on,
          player) -
          1 * str_detect(
            filter(pbp,event_type == "CHANGE" &
                     event_team_name %in% unique(pbp$away_team))$players_off,
            player)
      )
    )
    
    suppressMessages({away_skaters <- bind_cols(away_skaters, skaters_i)})
    rm(skaters_i, player)
  }
  
  colnames(away_skaters) <- rosters$playerId
  
  away_skaters <- data.frame(away_skaters)
  
  on_away <- which(away_skaters == 1, arr.ind = TRUE) %>%
    data.frame() %>%
    group_by(row) %>%
    summarize(
      away_on_1 = colnames(away_skaters)[unique(col)[1]],
      away_on_2 = colnames(away_skaters)[unique(col)[2]],
      away_on_3 = colnames(away_skaters)[unique(col)[3]],
      away_on_4 = colnames(away_skaters)[unique(col)[4]],
      away_on_5 = colnames(away_skaters)[unique(col)[5]],
      away_on_6 = colnames(away_skaters)[unique(col)[6]],
      away_on_7 = colnames(away_skaters)[unique(col)[7]]
    ) %>%
    mutate(
      across(
        .cols = away_on_1:away_on_7,
        ~as.integer(gsub("X","",.x))
      )
    )
  
  # Add on ice players to pbp
  pbp_full <- pbp %>%
    left_join(on_home, by = c("home_index" = "row")) %>%
    left_join(on_away, by = c("away_index" = "row"))
  
  # Select needed columns
  pbp_full <- pbp_full %>%
    mutate(season = pbp_full$season[1],
           game_date = pbp_full$game_date[1],
           game_id = pbp_full$game_id[1]) %>%
    #    filter(event != "Change") %>%
    mutate(event_id = str_pad(row_number(),width=4,side="left",pad=0),
           event_idx = as.numeric(paste0(game_id,event_id))) %>%
    select(season,game_date,game_id,event_idx,event_id,event,event_type,secondary_type,
           period,period_seconds,period_seconds_remaining,game_seconds,game_seconds_remaining,
           event_team_name,event_team_type,event_team_id,
           event_player_1_id,event_player_1_type,event_player_2_id,event_player_2_type,event_player_3_id,event_player_3_type,event_goalie_id,
           home_score,home_final,away_score,away_final,
           strength_state,empty_net,extra_attacker,home_skaters,away_skaters,
           home_on_1,home_on_2,home_on_3,home_on_4,home_on_5,home_on_6,home_on_7,
           away_on_1,away_on_2,away_on_3,away_on_4,away_on_5,away_on_6,away_on_7,
           x,y,shot_distance,shot_angle,zone_code,
           away_team,away_abbr,away_id,home_team,home_abbr,home_id)
  
  pbp_full$home_score[1] <- 0
  pbp_full$away_score[1] <- 0
  pbp_full$home_score <- zoo::na.locf(pbp_full$home_score)
  pbp_full$away_score <- zoo::na.locf(pbp_full$away_score)
  pbp_full$home_final <- last(pbp_full$home_score,na.rm = TRUE)
  pbp_full$away_final <- last(pbp_full$away_score,na.rm = TRUE)
  
  # Add xg
  pbp_full <- calculate_xg(pbp_full)
  
  return(pbp_full)
}
schedule <- function(date){
  url <- glue("https://api-web.nhle.com/v1/schedule/{date}")
  data_raw <- read_json(url)
  
  schedule_data <- data_raw$gameWeek %>%
    tibble() %>%
    unnest_wider(1) %>%
    unnest_longer(games) %>%
    unnest_wider(games) %>%
    select(date,season,id,gameType,awayTeam,homeTeam) %>%
    rename(game_id = id, game_type = gameType) %>%
    unnest_wider(awayTeam) %>%
    select(any_of(c("date","season","game_id","game_type",
                    away_id = "id",away_abbr = "abbrev",away_score = "score","homeTeam"))) %>%
    unnest_wider(homeTeam) %>%
    select(any_of(c("date","season","game_id","game_type","away_id","away_abbr","away_score",
                    home_id = "id",home_abbr = "abbrev",home_score="score")))
  
  return(schedule_data)
}
league_schedule <- function(start,end){
  league <- schedule(start)
  date <- as.Date(start)+7
  while(date < end){
    to_add <- schedule(date)
    league <- bind_rows(league,to_add)
    date <- as.Date(date)+7
  }
  league <- league %>% filter(game_type == 2)
  return(league)
}
skater_individual <- function(pbp,game_strength){
  power_play <- c("5v4","5v3","4v3","6v4","6v3")
  penalty_kill <- c("4v5","3v5","3v4","4v6","3v6")
  
  games <- pbp %>%
    filter(!is.na(event_player_1_id)) %>%
    group_by(playerID = event_player_1_id) %>%
    summarize(gp = length(unique(game_id)),
              .groups = "drop")
  
  if (game_strength == "pp"){
    pbp <- pbp %>% filter(strength_state %in% power_play)
  }
  if (game_strength == "pk"){
    pbp <- pbp %>% filter(strength_state %in% penalty_kill)
  }
  if (game_strength == "even") {
    pbp <- pbp %>% filter(strength_state == "5v5")
  }
  
  pbp <- pbp %>% filter(period < 5)
  
  
  ind <- pbp %>%
    filter(event_type %in% c("GOAL","SHOT_ON_GOAL","MISSED_SHOT","BLOCKED_SHOT","PENALTY")) %>%
    group_by(playerID = event_player_1_id) %>%
    summarize(ixg = sum(xg, na.rm = TRUE),
              goals = sum(event_type == "GOAL"),
              icf = sum(event_type %in% c("GOAL","SHOT_ON_GOAL","MISSED_SHOT","BLOCKED_SHOT")),
              iff = sum(event_type %in% c("GOAL","SHOT_ON_GOAL","MISSED_SHOT")),
              isog = sum(event_type %in% c("GOAL","SHOT_ON_GOAL")),
              gax = goals - ixg,
              pens_taken = sum(event_type == "PENALTY"),
              .groups = "drop"
    )
  a1 <- pbp %>%
    filter(event_type == "GOAL" & !is.na(event_player_2_id)) %>%
    group_by(playerID = event_player_2_id) %>%
    summarize(assists_prim = sum(event_type == "GOAL"),
              .groups = "drop")
  a2 <- pbp %>%
    filter(event_type == "GOAL" & !is.na(event_player_3_id)) %>%
    group_by(playerID = event_player_3_id) %>%
    summarize(assists_sec = sum(event_type == "GOAL"),
              .groups = "drop")
  pens_drawn <- pbp %>%
    filter(event_type == "PENALTY") %>%
    group_by(playerID = event_player_2_id) %>%
    summarize(pens_drawn = sum(event_type == "PENALTY"),
              .groups = "drop")
  
  player_stats <- games %>%
    full_join(ind, by="playerID") %>%
    full_join(a1, by="playerID") %>%
    full_join(a2, by="playerID") %>%
    full_join(pens_drawn, by="playerID") %>%
    mutate(
      across(
        .cols = everything(),
        ~replace(.x, is.na(.x), 0)
      )
    ) %>%
    mutate(
      goals = ifelse(is.na(goals), 0, goals),
      assists_prim = ifelse(is.na(assists_prim), 0, assists_prim),
      assists_sec = ifelse(is.na(assists_sec), 0, assists_sec),
      assists = assists_prim + assists_sec,
      points = goals + assists,
      points_primary = goals + assists_prim
    ) %>%
    select(
      playerID, gp, ixg, goals, assists, points, assists_prim, assists_sec, points_primary, gax, icf, iff, isog, pens_taken, pens_drawn
    )
  
  player_stats <- player_stats %>% drop_na()
  
  return(player_stats)
}
skater_onice <- function(pbp,game_strength){
  power_play <- c("5v4","5v3","4v3","6v4","6v3")
  penalty_kill <- c("4v5","3v5","3v4","4v6","3v6")
  
  if (game_strength == "pp"){
    pbp <- pbp %>% filter(strength_state %in% power_play)
  }
  if (game_strength == "pk"){
    pbp <- pbp %>% filter(strength_state %in% penalty_kill)
  }
  if (game_strength == "even") {
    pbp <- pbp %>% filter(strength_state == "5v5")
  }
  
  pbp <- pbp %>% 
    filter(period < 5) %>%
    filter(event_type %in% c("GOAL","SHOT_ON_GOAL","MISSED_SHOT","BLOCKED_SHOT"))
  
  pbp <- pbp %>%
    group_by(event_id) %>%
    mutate(
      home_players = paste(home_on_1,home_on_2,home_on_3,home_on_4,
                           home_on_5,home_on_6,home_on_7,sep = ";"),
      away_players = paste(away_on_1,away_on_2,away_on_3,away_on_4,
                           away_on_5,away_on_6,away_on_7,sep = ";")
    ) %>%
    ungroup() %>%
    select(game_id,event_id,home_team,away_team,event_team_name,event_type,xg,home_players,away_players)
  
  pbp$home_players <- str_remove_all(pbp$home_players,";NA")
  pbp$away_players <- str_remove_all(pbp$away_players,";NA")
  
  home_stats <- pbp %>%
    select(-away_players) %>%
    separate_rows(home_players,sep = ";") %>%
    mutate(xgf = xg * (event_team_name == home_team),
           xga = xg * (event_team_name == away_team),
           gf = ifelse(event_type == "GOAL" & event_team_name == home_team,1,0),
           ga = ifelse(event_type == "GOAL" & event_team_name == away_team,1,0),
           ff = ifelse(event_type != "MISSED_SHOT" & event_team_name == home_team,1,0),
           fa = ifelse(event_type != "MISSED_SHOT" & event_team_name == away_team,1,0)) %>%
    group_by(playerID = home_players) %>%
    summarise(
      gf = sum(gf),
      ga = sum(ga),
      xgf = sum(xgf, na.rm = TRUE),
      xga = sum(xga, na.rm = TRUE),
      cf = sum(event_team_name == home_team),
      ca = sum(event_team_name == away_team),
      ff = sum(ff),
      fa = sum(fa),
      .groups = "drop"
    )
  
  away_stats <- pbp %>%
    select(-home_players) %>%
    separate_rows(away_players,sep = ";") %>%
    mutate(xgf = xg * (event_team_name == away_team),
           xga = xg * (event_team_name == home_team),
           gf = ifelse(event_type == "GOAL" & event_team_name == away_team,1,0),
           ga = ifelse(event_type == "GOAL" & event_team_name == home_team,1,0),
           ff = ifelse(event_type != "MISSED_SHOT" & event_team_name == away_team,1,0),
           fa = ifelse(event_type != "MISSED_SHOT" & event_team_name == home_team,1,0)) %>%
    group_by(playerID = away_players) %>%
    summarise(
      gf = sum(gf),
      ga = sum(ga),
      xgf = sum(xgf, na.rm = TRUE),
      xga = sum(xga, na.rm = TRUE),
      cf = sum(event_team_name == away_team),
      ca = sum(event_team_name == home_team),
      ff = sum(ff),
      fa = sum(fa),
      .groups = "drop"
    )
  
  total <- bind_rows(home_stats,away_stats) %>%
    group_by(playerID) %>%
    summarise(
      gf = sum(gf),
      ga = sum(ga),
      xgf = sum(xgf),
      xga = sum(xga),
      cf = sum(cf),
      ca = sum(ca),
      ff = sum(ff),
      fa = sum(fa),
      .groups = "drop"
    )
  
  total$playerID = as.numeric(as.character(total$playerID))
  total <- total %>% drop_na()
  
  return(total)
  
}
goalie_stats <- function(pbp,game_strength){
  power_play <- c("5v4","5v3","4v3","6v4","6v3")
  penalty_kill <- c("4v5","3v5","3v4","4v6","3v6")
  
  if (game_strength == "pp"){
    pbp <- pbp %>% filter(strength_state %in% power_play)
  }
  if (game_strength == "pk"){
    pbp <- pbp %>% filter(strength_state %in% penalty_kill)
  }
  if (game_strength == "even") {
    pbp <- pbp %>% filter(strength_state == "5v5")
  }
  
  pbp <- pbp %>% 
    filter(period < 5) %>%
    filter(event_type %in% c("GOAL","SHOT_ON_GOAL","MISSED_SHOT","BLOCKED_SHOT"))
  
  goalie <- pbp %>%
    group_by(playerID = event_goalie_id) %>%
    summarise(
      gp = length(unique(game_id)),
      xga = sum(xg, na.rm = TRUE),
      ga = sum(event_type == "GOAL"),
      saves = sum(event_type == "SHOT_ON_GOAL"),
      shots_against = ga + saves,
      gsax = xga - ga,
      .groups = "drop"
    ) %>%
    select(playerID,gp,gsax,xga,ga,saves,shots_against) %>%
    drop_na()
  
  return(goalie)
  
}
player <- function(id){
  url <- glue("https://api-web.nhle.com/v1/player/{id}/landing")
  player_data <- read_json(url)
  
  player_id <- player_data$playerId %>%
    tibble() %>%
    rename(playerID = ".")
  first_name <- player_data$firstName %>%
    tibble() %>%
    dplyr::slice(1:1) %>%
    rename(first_name = ".")
  last_name <- player_data$lastName %>%
    tibble() %>%
    dplyr::slice(1:1) %>%
    rename(last_name = ".")
  position <- player_data$position %>%
    tibble() %>%
    rename(position_code = ".")
  active <- player_data$isActive %>%
    tibble() %>%
    rename(isActive = ".")
  if(active$isActive == TRUE){
    team <- player_data$currentTeamAbbrev %>%
      tibble() %>%
      rename(team = ".")
  }
  headshot <- player_data$headshot %>%
    tibble() %>%
    rename(headshot = ".")
  birthdate <- player_data$birthDate %>%
    tibble() %>%
    rename(birth_date = ".")
  height <- player_data$heightInInches %>%
    tibble() %>%
    rename(height = ".")
  weight <- player_data$weightInPounds %>%
    tibble() %>%
    rename(weight = ".")
  
  player_info <- if(active$isActive == TRUE){
    bind_cols(player_id,first_name,last_name,position,team,active,headshot,birthdate,height,weight)
  } else {
    bind_cols(player_id,first_name,last_name,position,active,headshot,birthdate,height,weight)
  }
  
  player_info$first_name <- unlist(player_info$first_name)
  player_info$last_name <- unlist(player_info$last_name)
  
  player_info <- player_info %>%
    mutate(
      position = ifelse(position_code %in% c("C","L","R"),"F",position_code), .before = position_code) %>%
    mutate(
      full_name = paste(first_name,last_name), .before = first_name,
      isActive = ifelse(isActive == TRUE,1,0)
    ) %>%
    select(-c(first_name,last_name))
  
  char_columns <- c("position","position_code","team")
  for(i in char_columns){
    if(i %not_in% names(player_info)){
      player_info[, i] <- NA_character_
    }
  }
  
  player_info <- player_info %>% select(playerID,full_name,position,position_code,team,isActive,headshot,birth_date,height,weight)
  
  return(player_info)
}
player_toi <- function(pbp){
  power_play <- c("toi_5v4","toi_5v3","toi_4v3","toi_6v4","toi_6v3","toi_7v5")
  penalty_kill <- c("toi_4v5","toi_3v5","toi_3v4","toi_4v6","toi_3v6","toi_5v7")
  even <- c("toi_5v5","toi_4v4","toi_3v3","toi_6v6","toi_6v5","toi_5v6")
  all <- c(power_play,penalty_kill,even)
  
  grouped_shifts <- group_shifts(pbp)
  
  toi <- grouped_shifts %>%
    ungroup() %>%
    select(game_id,offense_1,offense_2,offense_3,offense_4,offense_5,offense_6,offense_7,shift_length,strength_state) %>%
    pivot_longer(offense_1:offense_7,values_to = "playerID") %>%
    select(-name) %>%
    filter(playerID != "MISSING") %>%
    mutate(playerID = as.numeric(as.character(playerID))) %>%
    group_by(playerID,strength_state) %>%
    summarize(toi = round(sum(shift_length)/60,2),
              .groups = "drop") %>%
    pivot_wider(names_from = strength_state,names_prefix = "toi_",values_from = toi,values_fill = 0)
  
  for(i in all){
    if(i %not_in% names(toi)){
      toi[, i] <- 0
    }
  }
  
  toi <- toi %>%
    mutate(toi_all = toi_5v5+toi_4v4+toi_3v3+toi_6v6+toi_6v5+toi_5v6+toi_5v4+toi_5v3+toi_4v3+toi_6v4+toi_6v3+toi_4v5+toi_3v5+toi_3v4+toi_4v6+toi_3v6+toi_6v6+toi_7v5+toi_5v7,
           toi_even = toi_5v5+toi_4v4+toi_3v3+toi_6v6+toi_6v5+toi_5v6,
           toi_pp = toi_5v4+toi_5v3+toi_4v3+toi_6v4+toi_6v3+toi_7v5,
           toi_pk = toi_4v5+toi_3v5+toi_3v4+toi_4v6+toi_3v6+toi_5v7
    ) %>%
    select(playerID,toi_all,toi_even,toi_pp,toi_pk,toi_5v5,toi_4v4,toi_3v3,toi_6v6,toi_6v5,toi_5v6,toi_5v4,toi_5v3,toi_4v3,toi_6v4,toi_6v3,toi_7v5,toi_4v5,toi_3v5,toi_3v4,toi_4v6,toi_3v6,toi_5v7)
  
  return(toi)
}
group_shifts <- function(pbp){
  pbp <- pbp %>%
    filter(period < 5)
  
  pbp$home_on_1 <- ifelse(is.na(pbp$home_on_1), "MISSING", pbp$home_on_1)
  pbp$home_on_2 <- ifelse(is.na(pbp$home_on_2), "MISSING", pbp$home_on_2)
  pbp$home_on_3 <- ifelse(is.na(pbp$home_on_3), "MISSING", pbp$home_on_3)
  pbp$home_on_4 <- ifelse(is.na(pbp$home_on_4), "MISSING", pbp$home_on_4)
  pbp$home_on_5 <- ifelse(is.na(pbp$home_on_5), "MISSING", pbp$home_on_5)
  pbp$home_on_6 <- ifelse(is.na(pbp$home_on_6), "MISSING", pbp$home_on_6)
  pbp$home_on_7 <- ifelse(is.na(pbp$home_on_7), "MISSING", pbp$home_on_7)
  
  pbp$away_on_1 <- ifelse(is.na(pbp$away_on_1), "MISSING", pbp$away_on_1)
  pbp$away_on_2 <- ifelse(is.na(pbp$away_on_2), "MISSING", pbp$away_on_2)
  pbp$away_on_3 <- ifelse(is.na(pbp$away_on_3), "MISSING", pbp$away_on_3)
  pbp$away_on_4 <- ifelse(is.na(pbp$away_on_4), "MISSING", pbp$away_on_4)
  pbp$away_on_5 <- ifelse(is.na(pbp$away_on_5), "MISSING", pbp$away_on_5)
  pbp$away_on_6 <- ifelse(is.na(pbp$away_on_6), "MISSING", pbp$away_on_6)
  pbp$away_on_7 <- ifelse(is.na(pbp$away_on_7), "MISSING", pbp$away_on_7)
  
  pbp$shift_change <- ifelse(lag(pbp$home_on_1)==pbp$home_on_1 & lag(pbp$home_on_2)==pbp$home_on_2 & lag(pbp$home_on_3)==pbp$home_on_3 &
                               lag(pbp$home_on_4)==pbp$home_on_4 & lag(pbp$home_on_5)==pbp$home_on_5 & lag(pbp$home_on_6)==pbp$home_on_6 &
                               lag(pbp$away_on_1)==pbp$away_on_1 & lag(pbp$away_on_2)==pbp$away_on_2 & lag(pbp$away_on_3)==pbp$away_on_3 &
                               lag(pbp$away_on_4)==pbp$away_on_4 & lag(pbp$away_on_5)==pbp$away_on_5 & lag(pbp$away_on_6)==pbp$away_on_6 &
                               lag(pbp$home_score)==pbp$home_score & lag(pbp$away_score)==pbp$away_score & lag(pbp$period)==pbp$period &  
                               lag(pbp$game_id)==pbp$game_id, 0, 1)
  
  pbp$shift_change <- ifelse(is.na(pbp$shift_change), 0, pbp$shift_change)
  
  pbp$shift_change_index <- cumsum(pbp$shift_change)
  
  pbp <- pbp %>% mutate(game_score_state = glue("{home_score}v{away_score}"),
                        event_length = game_seconds - lag(game_seconds))
  pbp$home_xGF <- ifelse(pbp$event_team_name==pbp$home_team & !is.na(pbp$xg), pbp$xg, 0)
  pbp$away_xGF <- ifelse(pbp$event_team_name!=pbp$home_team & !is.na(pbp$xg), pbp$xg, 0)
  pbp$home_GF <- ifelse(pbp$event_type=="GOAL" & pbp$event_team_name==pbp$home_team, 1, 0)
  pbp$away_GF <- ifelse(pbp$event_type=="GOAL" & pbp$event_team_name==pbp$away_team, 1, 0)
  pbp$home_CF <- ifelse(pbp$event_type %in% c("GOAL","SHOT_ON_GOAL","MISSED_SHOT","BLOCKED_SHOT") & pbp$event_team_name==pbp$home_team, 1, 0)
  pbp$away_CF <- ifelse(pbp$event_type %in% c("GOAL","SHOT_ON_GOAL","MISSED_SHOT","BLOCKED_SHOT") & pbp$event_team_name==pbp$away_team, 1, 0)
  pbp$home_FF <- ifelse(pbp$event_type %in% c("GOAL","SHOT_ON_GOAL","MISSED_SHOT") & pbp$event_team_name==pbp$home_team, 1, 0)
  pbp$away_FF <- ifelse(pbp$event_type %in% c("GOAL","SHOT_ON_GOAL","MISSED_SHOT") & pbp$event_team_name==pbp$away_team, 1, 0)
  
  
  grouped_shifts <- pbp %>%
    group_by(game_id, shift_change_index, period, game_score_state, home_on_1, home_on_2, home_on_3, home_on_4, home_on_5, home_on_6, home_on_7,
             away_on_1, away_on_2, away_on_3, away_on_4, away_on_5, away_on_6, away_on_7) %>%
    summarise(shift_length = sum(event_length), homexGF = sum(home_xGF), awayxGF = sum(away_xGF), homeGF = sum(home_GF), awayGF = sum(away_GF),
              homeCF = sum(home_CF), awayCF = sum(away_CF), homeFF = sum(home_FF), awayFF = sum(away_FF)) %>%
    filter(shift_length > 0)
  
  home <- grouped_shifts %>%
    rename(offense_1 = home_on_1, offense_2 = home_on_2, offense_3 = home_on_3, offense_4 = home_on_4, offense_5 = home_on_5, offense_6 = home_on_6, offense_7 = home_on_7,
           defense_1 = away_on_1, defense_2 = away_on_2, defense_3 = away_on_3, defense_4 = away_on_4, defense_5 = away_on_5, defense_6 = away_on_6, defense_7 = away_on_7,
           xGF = homexGF, xGA = awayxGF, GF = homeGF, GA = awayGF, CF = homeCF, CA = awayCF, FF = homeFF, FA = awayFF) %>%
    select(game_id, shift_change_index, period, game_score_state,
           offense_1, offense_2, offense_3, offense_4, offense_5, offense_6, offense_7,
           defense_1, defense_2, defense_3, defense_4, defense_5, defense_6, defense_7,
           xGF, GF, CF, FF, xGA, GA, CA, FA, shift_length, shift_change_index)
  
  away <- grouped_shifts %>%
    rename(offense_1 = away_on_1, offense_2 = away_on_2, offense_3 = away_on_3, offense_4 = away_on_4, offense_5 = away_on_5, offense_6 = away_on_6, offense_7 = away_on_7,
           defense_1 = home_on_1, defense_2 = home_on_2, defense_3 = home_on_3, defense_4 = home_on_4, defense_5 = home_on_5, defense_6 = home_on_6, defense_7 = home_on_7,
           xGF = awayxGF, xGA = homexGF, GF = awayGF, GA = homeGF, CF = awayCF, CA = homeCF, FF = awayFF, FA = homeFF) %>%
    select(game_id, shift_change_index, period, game_score_state,
           offense_1, offense_2, offense_3, offense_4, offense_5, offense_6, offense_7,
           defense_1, defense_2, defense_3, defense_4, defense_5, defense_6, defense_7,
           xGF, GF, CF, FF, xGA, GA, CA, FA, shift_length, shift_change_index)
  
  shifts_combined <- full_join(home, away) %>% arrange(shift_change_index) %>%
    mutate(offense_players = paste(offense_1,offense_2,offense_3,offense_4,offense_5,offense_6,offense_7),
           defense_players = paste(defense_1,defense_2,defense_3,defense_4,defense_5,defense_6,defense_7))
  
  shifts_combined$offense_players <- gsub(" MISSING","",shifts_combined$offense_players)
  shifts_combined$defense_players <- gsub(" MISSING","",shifts_combined$defense_players)
  shifts_combined$strength_state <- paste(str_count(shifts_combined$offense_players," ") + 1,str_count(shifts_combined$defense_players," ") + 1,sep = "v")
  
  return(shifts_combined)
}
team_roster <- function(team){
  url <- glue("https://api-web.nhle.com/v1/roster/{team}/current")
  data_raw <- read_json(url)
  
  forward_data <- data_raw$forwards %>%
    tibble() %>%
    unnest_wider(1)
  first_name <- forward_data$firstName %>%
    tibble() %>%
    unnest_wider(1) %>%
    select(firstName = default)
  last_name <- forward_data$lastName %>%
    tibble() %>%
    unnest_wider(1) %>%
    select(lastName = default)
  forward_data <- forward_data %>%
    select(id,positionCode) %>%
    bind_cols(first_name,last_name) %>%
    relocate(firstName,lastName,.after = id) %>%
    mutate(position = ifelse(positionCode %in% c("C","L","R"),"F",positionCode), .before = positionCode)
  
  
  defense_data <- data_raw$defensemen %>%
    tibble() %>%
    unnest_wider(1)
  first_name <- defense_data$firstName %>%
    tibble() %>%
    unnest_wider(1) %>%
    select(firstName = default)
  last_name <- defense_data$lastName %>%
    tibble() %>%
    unnest_wider(1) %>%
    select(lastName = default)
  defense_data <- defense_data %>%
    select(id,positionCode) %>%
    bind_cols(first_name,last_name) %>%
    relocate(firstName,lastName,.after = id) %>%
    mutate(position = ifelse(positionCode %in% c("C","L","R"),"F",positionCode), .before = positionCode)
  
  
  goalie_data <- data_raw$goalies %>%
    tibble() %>%
    unnest_wider(1)
  first_name <- goalie_data$firstName %>%
    tibble() %>%
    unnest_wider(1) %>%
    select(firstName = default)
  last_name <- goalie_data$lastName %>%
    tibble() %>%
    unnest_wider(1) %>%
    select(lastName = default)
  goalie_data <- goalie_data %>%
    select(id,positionCode) %>%
    bind_cols(first_name,last_name) %>%
    relocate(firstName,lastName,.after = id) %>%
    mutate(position = ifelse(positionCode %in% c("C","L","R"),"F",positionCode), .before = positionCode)
  
  roster_data <- bind_rows(forward_data,defense_data,goalie_data) %>%
    mutate(team = team)
  return(roster_data)
  
}
team_standings <- function(){
  url <- glue("https://api-web.nhle.com/v1/standings/now")
  team_data <- read_json(url)
  
  standings_data <- team_data$standings %>%
    tibble() %>%
    unnest_wider(1) %>%
    unnest_wider(placeName)
  
  standings_data <- standings_data %>%
    select(teamName=default,teamAbbrev,conferenceName,divisionName,gamesPlayed,wins,losses,otLosses,points,pointPctg,regulationWins,
           regulationPlusOtWins,goalFor,goalAgainst,goalDifferential) %>%
    unnest_wider(teamAbbrev) %>%
    rename(teamAbbrev = default)
  
  return(standings_data)
}
team_data <- function(pbp,game_strength){
  power_play <- c("5v4","5v3","4v3","6v4","6v3")
  penalty_kill <- c("4v5","3v5","3v4","4v6","3v6")
  
  if (game_strength == "pp"){
    pbp <- pbp %>% filter(strength_state %in% power_play)
  }
  if (game_strength == "pk"){
    pbp <- pbp %>% filter(strength_state %in% penalty_kill)
  }
  if (game_strength == "even") {
    pbp <- pbp %>% filter(strength_state == "5v5")
  }
  
  pbp <- pbp %>% 
    filter(period < 5) %>%
    filter(event_type %in% c("GOAL","SHOT_ON_GOAL","MISSED_SHOT","BLOCKED_SHOT")) %>%
    mutate(
      home_xgf = ifelse(event_team_name == home_team,xg,0),
      away_xgf = ifelse(event_team_name == away_team,xg,0),
      home_gf = ifelse(event_type == "GOAL" & event_team_name == home_team,1,0),
      away_gf = ifelse(event_type == "GOAL" & event_team_name == away_team,1,0),
      home_sf = ifelse(event_type %in% c("GOAL","SHOT_ON_GOAL") & event_team_name == home_team,1,0),
      away_sf = ifelse(event_type %in% c("GOAL","SHOT_ON_GOAL") & event_team_name == away_team,1,0),
      home_cf = ifelse(event_team_name == home_team,1,0),
      away_cf = ifelse(event_team_name == away_team,1,0),
      home_ff = ifelse(event_type != "MISSED_SHOT" & event_team_name == home_team,1,0),
      away_ff = ifelse(event_type != "MISSED_SHOT" & event_team_name == away_team,1,0)
    )
  
  home_stats <- pbp %>%
    group_by(team = home_team) %>%
    summarize(
      gf = sum(home_gf),
      ga = sum(away_gf),
      xgf = sum(home_xgf,na.rm = TRUE),
      xga = sum(away_xgf,na.rm = TRUE),
      sf = sum(home_sf),
      sa = sum(away_sf),
      cf = sum(home_cf),
      ca = sum(away_cf),
      ff = sum(home_ff),
      fa = sum(away_ff)
    )
  
  away_stats <- pbp %>%
    group_by(team = away_team) %>%
    summarize(
      gf = sum(away_gf),
      ga = sum(home_gf),
      xgf = sum(away_xgf,na.rm = TRUE),
      xga = sum(home_xgf,na.rm = TRUE),
      sf = sum(away_sf),
      sa = sum(home_sf),
      cf = sum(away_cf),
      ca = sum(home_cf),
      ff = sum(away_ff),
      fa = sum(home_ff)
    )
  
  data <- bind_rows(home_stats,away_stats) %>%
    group_by(team) %>%
    summarize(
      gf = sum(gf),
      ga = sum(ga),
      xgf = sum(xgf),
      xga = sum(xga),
      sf = sum(sf),
      sa = sum(sa),
      cf = sum(cf),
      ca = sum(ca),
      ff = sum(ff),
      fa = sum(fa)
    )
  
  return(data)
}
toi_teams <- function(pbp){
  pbp <- pbp %>%
    filter(period < 5)
  
  pbp$home_on_1 <- ifelse(is.na(pbp$home_on_1), "MISSING", pbp$home_on_1)
  pbp$home_on_2 <- ifelse(is.na(pbp$home_on_2), "MISSING", pbp$home_on_2)
  pbp$home_on_3 <- ifelse(is.na(pbp$home_on_3), "MISSING", pbp$home_on_3)
  pbp$home_on_4 <- ifelse(is.na(pbp$home_on_4), "MISSING", pbp$home_on_4)
  pbp$home_on_5 <- ifelse(is.na(pbp$home_on_5), "MISSING", pbp$home_on_5)
  pbp$home_on_6 <- ifelse(is.na(pbp$home_on_6), "MISSING", pbp$home_on_6)
  pbp$home_on_7 <- ifelse(is.na(pbp$home_on_7), "MISSING", pbp$home_on_7)
  
  pbp$away_on_1 <- ifelse(is.na(pbp$away_on_1), "MISSING", pbp$away_on_1)
  pbp$away_on_2 <- ifelse(is.na(pbp$away_on_2), "MISSING", pbp$away_on_2)
  pbp$away_on_3 <- ifelse(is.na(pbp$away_on_3), "MISSING", pbp$away_on_3)
  pbp$away_on_4 <- ifelse(is.na(pbp$away_on_4), "MISSING", pbp$away_on_4)
  pbp$away_on_5 <- ifelse(is.na(pbp$away_on_5), "MISSING", pbp$away_on_5)
  pbp$away_on_6 <- ifelse(is.na(pbp$away_on_6), "MISSING", pbp$away_on_6)
  pbp$away_on_7 <- ifelse(is.na(pbp$away_on_7), "MISSING", pbp$away_on_7)
  
  pbp$shift_change <- ifelse(lag(pbp$home_on_1)==pbp$home_on_1 & lag(pbp$home_on_2)==pbp$home_on_2 & lag(pbp$home_on_3)==pbp$home_on_3 &
                               lag(pbp$home_on_4)==pbp$home_on_4 & lag(pbp$home_on_5)==pbp$home_on_5 & lag(pbp$home_on_6)==pbp$home_on_6 &
                               lag(pbp$away_on_1)==pbp$away_on_1 & lag(pbp$away_on_2)==pbp$away_on_2 & lag(pbp$away_on_3)==pbp$away_on_3 &
                               lag(pbp$away_on_4)==pbp$away_on_4 & lag(pbp$away_on_5)==pbp$away_on_5 & lag(pbp$away_on_6)==pbp$away_on_6 &
                               lag(pbp$home_score)==pbp$home_score & lag(pbp$away_score)==pbp$away_score & lag(pbp$period)==pbp$period &  
                               lag(pbp$game_id)==pbp$game_id, 0, 1)
  pbp$shift_change <- ifelse(is.na(pbp$shift_change), 0, pbp$shift_change)
  pbp$shift_change_index <- cumsum(pbp$shift_change)
  pbp <- pbp %>%
    mutate(game_score_state = glue("{home_score}v{away_score}"),
           event_length = game_seconds - lag(game_seconds))
  pbp$home_team[pbp$home_team==""] <- NA
  pbp$away_team[pbp$away_team==""] <- NA
  pbp <- pbp %>%
    fill(home_team) %>%
    fill(away_team)
  
  grouped_shifts <- pbp %>%
    group_by(game_id, shift_change_index, period, game_score_state, home_on_1, home_on_2, home_on_3, home_on_4, home_on_5, home_on_6, home_on_7,
             away_on_1, away_on_2, away_on_3, away_on_4, away_on_5, away_on_6, away_on_7,home_team,away_team) %>%
    summarise(shift_length = sum(event_length)) %>%
    filter(shift_length > 0)
  
  home <- grouped_shifts %>%
    ungroup() %>%
    rename(offense_1 = home_on_1, offense_2 = home_on_2, offense_3 = home_on_3, offense_4 = home_on_4, offense_5 = home_on_5, offense_6 = home_on_6, offense_7 = home_on_7,
           defense_1 = away_on_1, defense_2 = away_on_2, defense_3 = away_on_3, defense_4 = away_on_4, defense_5 = away_on_5, defense_6 = away_on_6, defense_7 = away_on_7,
           team = home_team) %>%
    select(game_id, shift_change_index, period, game_score_state,
           offense_1, offense_2, offense_3, offense_4, offense_5, offense_6, offense_7,
           defense_1, defense_2, defense_3, defense_4, defense_5, defense_6, defense_7, shift_length, shift_change_index,team)
  
  away <- grouped_shifts %>%
    ungroup() %>%
    rename(offense_1 = away_on_1, offense_2 = away_on_2, offense_3 = away_on_3, offense_4 = away_on_4, offense_5 = away_on_5, offense_6 = away_on_6, offense_7 = away_on_7,
           defense_1 = home_on_1, defense_2 = home_on_2, defense_3 = home_on_3, defense_4 = home_on_4, defense_5 = home_on_5, defense_6 = home_on_6, defense_7 = home_on_7,
           team=away_team) %>%
    select(game_id, shift_change_index, period, game_score_state,
           offense_1, offense_2, offense_3, offense_4, offense_5, offense_6, offense_7,
           defense_1, defense_2, defense_3, defense_4, defense_5, defense_6, defense_7, shift_length, shift_change_index,team)
  
  shifts_combined <- full_join(home, away) %>% arrange(shift_change_index) %>%
    mutate(offense_players = paste(offense_1,offense_2,offense_3,offense_4,offense_5,offense_6,offense_7),
           defense_players = paste(defense_1,defense_2,defense_3,defense_4,defense_5,defense_6,defense_7))
  shifts_combined$offense_players <- gsub(" MISSING","",shifts_combined$offense_players)
  shifts_combined$defense_players <- gsub(" MISSING","",shifts_combined$defense_players)
  shifts_combined$strength_state <- paste(str_count(shifts_combined$offense_players," ") + 1,str_count(shifts_combined$defense_players," ") + 1,sep = "v")
  
  power_play <- c("toi_5v4","toi_5v3","toi_4v3","toi_6v4","toi_6v3","toi_7v5")
  penalty_kill <- c("toi_4v5","toi_3v5","toi_3v4","toi_4v6","toi_3v6","toi_5v7")
  even <- c("toi_5v5","toi_4v4","toi_3v3","toi_6v6","toi_6v5","toi_5v6")
  all <- c(power_play,penalty_kill,even)
  
  toi <- shifts_combined %>%
    ungroup() %>%
    select(game_id,team,shift_length,strength_state) %>%
    group_by(team,strength_state) %>%
    summarize(toi = round(sum(shift_length)/60,2),
              .groups = "drop") %>%
    pivot_wider(names_from = strength_state,names_prefix = "toi_",values_from = toi,values_fill = 0)
  
  for(i in all){
    if(i %not_in% names(toi)){
      toi[, i] <- 0
    }
  }
  
  toi <- toi %>%
    mutate(toi_all = toi_5v5+toi_4v4+toi_3v3+toi_6v6+toi_6v5+toi_5v6+toi_5v4+toi_5v3+toi_4v3+toi_6v4+toi_6v3+toi_4v5+toi_3v5+toi_3v4+toi_4v6+toi_3v6+toi_6v6+toi_7v5+toi_5v7,
           toi_even = toi_5v5+toi_4v4+toi_3v3+toi_6v6+toi_6v5+toi_5v6,
           toi_pp = toi_5v4+toi_5v3+toi_4v3+toi_6v4+toi_6v3+toi_7v5,
           toi_pk = toi_4v5+toi_3v5+toi_3v4+toi_4v6+toi_3v6+toi_5v7
    ) %>%
    select(team,toi_all,toi_even,toi_pp,toi_pk,toi_5v5,toi_4v4,toi_3v3,toi_6v6,toi_6v5,toi_5v6,toi_5v4,toi_5v3,toi_4v3,toi_6v4,toi_6v3,toi_7v5,toi_4v5,toi_3v5,toi_3v4,toi_4v6,toi_3v6,toi_5v7)
  
  return(toi)
}

#### PBP #######################################################################
pbp <- readRDS(url("https://github.com/zackkehl/HockeyZK_dataupdates/raw/main/data/pbp_24_25.rds"))

schedule_24_25 <- league_schedule("2024-09-29","2025-04-17")
completed_games <- schedule_24_25 %>% filter(home_score+away_score>0)
games_to_scrape <- setdiff(completed_games$game_id,unique(pbp$game_id))

if(length(games_to_scrape)>0){
  for(i in 1:length(games_to_scrape)){
    print(i)
    game <- games_to_scrape[i]
    game_data <- game_scraper_html(game)
    if(sum(is.na(game_data$home_on_1)) < 40){pbp <- rbind(pbp,game_data)}
  }
  
  games_to_scrape <- setdiff(completed_games$game_id,unique(pbp$game_id))
  
  if(length(games_to_scrape)>0){
    for(i in 1:length(games_to_scrape)){
      skip_to_next <- FALSE
      print(i)
      game <- games_to_scrape[i]
      game_data <- tryCatch(game_scraper(game), error = function(e) { skip_to_next <<- TRUE})
      if(skip_to_next) { next }
      pbp <- rbind(pbp,game_data)
    }
  }
  
  pbp <- pbp %>% arrange(event_idx)
}

saveRDS(pbp,"data/pbp_24_25.rds")
################################################################################




