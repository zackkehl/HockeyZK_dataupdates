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

pbp %>% saveRDS("pbp_24_25.rds")
################################################################################




