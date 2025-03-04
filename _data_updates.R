library(DBI)
library(RMySQL)
library(dplyr)
library(tidyr)
library(readr)
library(purrr)
library(stringr)
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

host <- "localhost"
user <- "root"
password <- "HockeyZKMySQL88"
database <- "hockeyzk"

con <- dbConnect(RMySQL::MySQL(), dbname = database, host = host, username = user, password = password)

dbGetQuery(con,'
  SET GLOBAL local_infile=1
')

#### PBP #######################################################################
pbp <- dbReadTable(con,"pbp_24_25")

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

dbWriteTable(con, name = "pbp_24_25", value = pbp, row.names = FALSE, overwrite = TRUE)
################################################################################

#### Player/Goalie Data - Single Season ########################################
pbp <- dbReadTable(con,"pbp_24_25")
plays <- pbp

toi <- player_toi(pbp)
toi_all <- toi %>% select(playerID,toi_all) %>% rename(toi = toi_all)
toi_5v5 <- toi %>% select(playerID,toi_5v5) %>% rename(toi = toi_5v5)
toi_pp <- toi %>% select(playerID,toi_pp) %>% rename(toi = toi_pp)
toi_pk <- toi %>% select(playerID,toi_pk) %>% rename(toi = toi_pk)
toi_threshold_even <- min(250,(length(unique(pbp$game_id))/16)*10)
toi_threshold_spec <- min(75,(length(unique(pbp$game_id))/16)*2)

individual_all <- skater_individual(pbp,"all") %>%
  left_join(toi_all) %>%
  mutate(g_60 = (goals/toi)*60,prim_a_60 = (assists_prim/toi)*60,sec_a_60 = (assists_sec/toi)*60,ixg_60 = (ixg/toi)*60,icf_60 = (icf/toi)*60)
individual_all <- na.omit(individual_all)
onice_all <- skater_onice(pbp,"all") %>%
  left_join(toi_all) %>%
  mutate(gf_perc = gf/(gf+ga),xgf_perc = xgf/(xgf+xga),cf_perc = cf/(cf+ca),gf_60 = (gf/toi)*60,ga_60 = (ga/toi)*60,xgf_60 = (xgf/toi)*60,xga_60 = (xga/toi)*60,cf_60 = (cf/toi)*60,ca_60 = (ca/toi)*60) %>%
  select(-toi)
onice_all <- na.omit(onice_all)
player_summary <- individual_all %>% left_join(onice_all,by="playerID")
player_summary <- na.omit(player_summary)
all_summary <- player_summary %>%
  mutate(game_score = ((0.75*goals) + (0.7*assists_prim) + (0.55*assists_sec) + (0.075*isog) + (0.1*ixg) + (0.05*cf) + (0.1*xgf) + (0.15*gf) - (0.05*ca) - (0.1*xga) - (0.15*xga)) / gp)
rm(individual_all,onice_all)

even_strength <- c("3v3", "4v4", "5v5")
power_play <- c("5v4","5v3","4v3","6v4","6v3")
penalty_kill <- c("4v5","3v5","3v4","4v6","3v6")

pbp <- pbp %>%
  filter(period < 5)

pbp$home_on_1 <- ifelse(is.na(pbp$home_on_1), "MISSING", pbp$home_on_1)
pbp$home_on_2 <- ifelse(is.na(pbp$home_on_2), "MISSING", pbp$home_on_2)
pbp$home_on_3 <- ifelse(is.na(pbp$home_on_3), "MISSING", pbp$home_on_3)
pbp$home_on_4 <- ifelse(is.na(pbp$home_on_4), "MISSING", pbp$home_on_4)
pbp$home_on_5 <- ifelse(is.na(pbp$home_on_5), "MISSING", pbp$home_on_5)
pbp$home_on_6 <- ifelse(is.na(pbp$home_on_6), "MISSING", pbp$home_on_6)
pbp$away_on_1 <- ifelse(is.na(pbp$away_on_1), "MISSING", pbp$away_on_1)
pbp$away_on_2 <- ifelse(is.na(pbp$away_on_2), "MISSING", pbp$away_on_2)
pbp$away_on_3 <- ifelse(is.na(pbp$away_on_3), "MISSING", pbp$away_on_3)
pbp$away_on_4 <- ifelse(is.na(pbp$away_on_4), "MISSING", pbp$away_on_4)
pbp$away_on_5 <- ifelse(is.na(pbp$away_on_5), "MISSING", pbp$away_on_5)
pbp$away_on_6 <- ifelse(is.na(pbp$away_on_6), "MISSING", pbp$away_on_6)

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
pbp$home_xGF <- ifelse(pbp$event_team_name==pbp$home_team, pbp$xg, 0)
pbp$away_xGF <- ifelse(pbp$event_team_name!=pbp$home_team, pbp$xg, 0)
pbp$home_GF <- ifelse(pbp$event_type=="GOAL" & pbp$event_team_name==pbp$home_team, 1, 0)
pbp$away_GF <- ifelse(pbp$event_type=="GOAL" & pbp$event_team_name==pbp$away_team, 1, 0)
pbp$home_CF <- ifelse(pbp$event_type %in% c("GOAL","SHOT_ON_GOAL","MISSED_SHOT","BLOCKED_SHOT") & pbp$event_team_name==pbp$home_team, 1, 0)
pbp$away_CF <- ifelse(pbp$event_type %in% c("GOAL","SHOT_ON_GOAL","MISSED_SHOT","BLOCKED_SHOT") & pbp$event_team_name==pbp$away_team, 1, 0)
pbp$tied <- ifelse(pbp$home_score==pbp$away_score, 1, 0)
pbp$home_lead_1 <- ifelse(pbp$home_score-pbp$away_score==1, 1, 0)
pbp$home_lead_2 <- ifelse(pbp$home_score-pbp$away_score==2, 1, 0)
pbp$home_lead_3 <- ifelse(pbp$home_score-pbp$away_score>=3, 1, 0)
pbp$away_lead_1 <- ifelse(pbp$home_score-pbp$away_score==(-1), 1, 0)
pbp$away_lead_2 <- ifelse(pbp$home_score-pbp$away_score==(-2), 1, 0)
pbp$away_lead_3 <- ifelse(pbp$home_score-pbp$away_score<=(-3), 1, 0)
pbp$Five <- ifelse(pbp$strength_state=="5v5", 1, 0)
pbp$Four <- ifelse(pbp$strength_state=="4v4", 1, 0)
pbp$Three <- ifelse(pbp$strength_state=="3v3", 1, 0)
pbp$home_xGF[is.na(pbp$home_xGF)] <- 0
pbp$away_xGF[is.na(pbp$away_xGF)] <- 0
pbp$event_length[is.na(pbp$event_length)] <- 0

pbp_ev <- pbp %>%
  filter(strength_state %in% even_strength)

grouped_shifts <- pbp_ev %>%
  group_by(game_id, shift_change_index, period, game_score_state, home_on_1, home_on_2, home_on_3, home_on_4, home_on_5, home_on_6, 
           away_on_1, away_on_2, away_on_3, away_on_4, away_on_5, away_on_6) %>%
  summarise(shift_length = sum(event_length), homexGF = sum(home_xGF), awayxGF = sum(away_xGF),
            homeGF = sum(home_GF), awayGF = sum(away_GF), homeCF = sum(home_CF), awayCF = sum(away_CF),
            Home_Up_1 = max(home_lead_1), Home_Up_2 = max(home_lead_2), Home_Up_3 = max(home_lead_3),
            Away_Up_1 = max(away_lead_1), Away_Up_2 = max(away_lead_2), Away_Up_3 = max(away_lead_3), Tied = max(tied), 
            State_5v5 = max(Five), State_4v4 = max(Four), State_3v3 = max(Three)) %>%
  filter(shift_length > 0)

home_as_off <- grouped_shifts %>%
  rename(offense_1 = home_on_1, offense_2 = home_on_2, offense_3 = home_on_3, offense_4 = home_on_4, offense_5 = home_on_5, offense_6 = home_on_6,
         defense_1 = away_on_1, defense_2 = away_on_2, defense_3 = away_on_3, defense_4 = away_on_4, defense_5 = away_on_5, defense_6 = away_on_6,
         Up_1 = Home_Up_1, Up_2 = Home_Up_2, Up_3 = Home_Up_3, Down_1 = Away_Up_1, Down_2 = Away_Up_2, Down_3 = Away_Up_3, xGF = homexGF, GF = homeGF, CF = homeCF) %>%
  select(game_id, shift_change_index, period, game_score_state,
         offense_1, offense_2, offense_3, offense_4, offense_5, offense_6, 
         defense_1, defense_2, defense_3, defense_4, defense_5, defense_6, 
         xGF, GF, CF, shift_length, shift_change_index, Tied, State_5v5, State_4v4, State_3v3, 
         Up_1, Up_2, Up_3, Down_1, Down_2, Down_3) %>%
  mutate(xGF_60 = xGF*3600/shift_length, GF_60 = GF*3600/shift_length, CF_60 = CF*3600/shift_length, is_home = 1)

away_as_off <- grouped_shifts %>%
  rename(offense_1 = away_on_1, offense_2 = away_on_2, offense_3 = away_on_3, offense_4 = away_on_4, offense_5 = away_on_5, offense_6 = away_on_6,
         defense_1 = home_on_1, defense_2 = home_on_2, defense_3 = home_on_3, defense_4 = home_on_4, defense_5 = home_on_5, defense_6 = home_on_6,
         Up_1 = Away_Up_1, Up_2 = Away_Up_2, Up_3 = Away_Up_3, Down_1 = Home_Up_1, Down_2 = Home_Up_2, Down_3 = Home_Up_3, xGF = awayxGF, GF = awayGF, CF = awayCF) %>%
  select(game_id, shift_change_index, period, game_score_state,
         offense_1, offense_2, offense_3, offense_4, offense_5, offense_6, 
         defense_1, defense_2, defense_3, defense_4, defense_5, defense_6, 
         xGF, GF, CF, shift_length, shift_change_index, Tied, State_5v5, State_4v4, State_3v3, 
         Up_1, Up_2, Up_3, Down_1, Down_2, Down_3) %>%
  mutate(xGF_60 = xGF*3600/shift_length, GF_60 = GF*3600/shift_length, CF_60 = CF*3600/shift_length, is_home = 0)

shifts_combined <- full_join(home_as_off, away_as_off)
rm(pbp_ev,home_as_off,away_as_off,grouped_shifts)

shifts_subset = subset(shifts_combined, select = c(offense_1:offense_6,defense_1:defense_6,shift_length,State_4v4,State_3v3,Up_1:Down_3,xGF_60,GF_60,CF_60,is_home))

shifts_combined_dummies_off <- dummy_cols(shifts_subset, select_columns = c("offense_1","offense_2","offense_3","offense_4","offense_5","offense_6"))
shifts_combined_dummies_def <- dummy_cols(shifts_subset, select_columns = c("defense_1","defense_2","defense_3","defense_4","defense_5","defense_6"))
shifts_combined_dummies_off = subset(shifts_combined_dummies_off, select = -c(offense_1,offense_2,offense_3,offense_4,offense_5,offense_6,defense_1,defense_2,defense_3,defense_4,defense_5,defense_6))
shifts_combined_dummies_def = subset(shifts_combined_dummies_def, select = -c(offense_1,offense_2,offense_3,offense_4,offense_5,offense_6,defense_1,defense_2,defense_3,defense_4,defense_5,defense_6,shift_length,State_4v4,State_3v3,Up_1:Down_3,xGF_60,GF_60,CF_60,is_home))
shifts_combined_dummies <- cbind(shifts_combined_dummies_off, shifts_combined_dummies_def)
rm(shifts_subset,shifts_combined_dummies_off,shifts_combined_dummies_def,shifts_combined)

colnames(shifts_combined_dummies) = gsub("offense_1", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("offense_2", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("offense_3", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("offense_4", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("offense_5", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("offense_6", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_1", "defense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_2", "defense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_3", "defense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_4", "defense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_5", "defense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_6", "defense_", colnames(shifts_combined_dummies))

shifts_combined_dummies <- as.data.frame(lapply(split.default(shifts_combined_dummies, names(shifts_combined_dummies)), function(x) Reduce(`+`, x)))

shifts_combined_dummies <- shifts_combined_dummies %>% select(-contains("Goalie"))
shifts_combined_dummies <- shifts_combined_dummies %>% select(-contains("Missing"))

xGF60 <- as.numeric(c(shifts_combined_dummies$xGF_60))
GF60 <- as.numeric(c(shifts_combined_dummies$GF_60))
CF60 <- as.numeric(c(shifts_combined_dummies$CF_60))
shift_length <- as.numeric(c(shifts_combined_dummies$shift_length))
subsetted_dummies = subset(shifts_combined_dummies, select = -c(shift_length, xGF_60, GF_60, CF_60))

RAPM_xGF <- as.matrix(subsetted_dummies)
Sparse_RAPM_xGF <- Matrix(RAPM_xGF, sparse = TRUE)
rm(RAPM_xGF)

RAPM_GF <- as.matrix(subsetted_dummies)
Sparse_RAPM_GF <- Matrix(RAPM_GF, sparse = TRUE)
rm(RAPM_GF)

RAPM_CF <- as.matrix(subsetted_dummies)
Sparse_RAPM_CF <- Matrix(RAPM_CF, sparse = TRUE)
rm(RAPM_CF)

rm(shifts_combined_dummies,subsetted_dummies)
Cross_Validated_Results_xGF <- cv.glmnet(x=Sparse_RAPM_xGF, y=xGF60, weights=shift_length, alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)
Cross_Validated_Results_GF <- cv.glmnet(x=Sparse_RAPM_GF, y=GF60, weights=shift_length, alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)
Cross_Validated_Results_CF <- cv.glmnet(x=Sparse_RAPM_CF, y=CF60, weights=shift_length, alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)

Run_RAPM_xGF <- glmnet(x=Sparse_RAPM_xGF, y=xGF60, weights=shift_length, lambda = Cross_Validated_Results_xGF[["lambda.min"]], alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)
Run_RAPM_GF <- glmnet(x=Sparse_RAPM_GF, y=GF60, weights=shift_length, lambda = Cross_Validated_Results_xGF[["lambda.min"]], alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)
Run_RAPM_CF <- glmnet(x=Sparse_RAPM_CF, y=CF60, weights=shift_length, lambda = Cross_Validated_Results_xGF[["lambda.min"]], alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)

RAPM_xGF_coefficients <- as.data.frame(as.matrix(coef(Run_RAPM_xGF)))
RAPM_GF_coefficients <- as.data.frame(as.matrix(coef(Run_RAPM_GF)))
RAPM_CF_coefficients <- as.data.frame(as.matrix(coef(Run_RAPM_CF)))

Binded_Coefficients_xGF <- cbind(rownames(RAPM_xGF_coefficients), RAPM_xGF_coefficients) %>%
  rename(Player = `rownames(RAPM_xGF_coefficients)`, xGF_60 = s0)
Binded_Coefficients_GF <- cbind(rownames(RAPM_GF_coefficients), RAPM_GF_coefficients) %>%
  rename(Player = `rownames(RAPM_GF_coefficients)`, GF_60 = s0)
Binded_Coefficients_CF <- cbind(rownames(RAPM_CF_coefficients), RAPM_CF_coefficients) %>%
  rename(Player = `rownames(RAPM_CF_coefficients)`, CF_60 = s0)

offense_RAPM_xGF <- Binded_Coefficients_xGF %>%
  filter(grepl("offense", Binded_Coefficients_xGF$Player))
offense_RAPM_GF <- Binded_Coefficients_GF %>%
  filter(grepl("offense", Binded_Coefficients_GF$Player))
offense_RAPM_CF <- Binded_Coefficients_CF %>%
  filter(grepl("offense", Binded_Coefficients_CF$Player))

offense_RAPM_xGF$Player = str_replace_all(offense_RAPM_xGF$Player, "offense__", "")
offense_RAPM_GF$Player = str_replace_all(offense_RAPM_GF$Player, "offense__", "")
offense_RAPM_CF$Player = str_replace_all(offense_RAPM_CF$Player, "offense__", "")

defense_RAPM_xGA <- Binded_Coefficients_xGF %>%
  filter(grepl("defense", Binded_Coefficients_xGF$Player)) %>%
  rename(xGA_60 = xGF_60)
defense_RAPM_GA <- Binded_Coefficients_GF %>%
  filter(grepl("defense", Binded_Coefficients_GF$Player)) %>%
  rename(GA_60 = GF_60)
defense_RAPM_CA <- Binded_Coefficients_CF %>%
  filter(grepl("defense", Binded_Coefficients_CF$Player)) %>%
  rename(CA_60 = CF_60)

defense_RAPM_xGA$Player = str_replace_all(defense_RAPM_xGA$Player, "defense__", "")
defense_RAPM_GA$Player = str_replace_all(defense_RAPM_GA$Player, "defense__", "")
defense_RAPM_CA$Player = str_replace_all(defense_RAPM_CA$Player, "defense__", "")

joined_RAPM_xG <- inner_join(offense_RAPM_xGF, defense_RAPM_xGA, by="Player")
joined_RAPM_G <- inner_join(offense_RAPM_GF, defense_RAPM_GA, by="Player")
joined_RAPM_C <- inner_join(offense_RAPM_CF, defense_RAPM_CA, by="Player")

joined_RAPM_xG$xGPM_60 <- joined_RAPM_xG$xGF_60 - joined_RAPM_xG$xGA_60
joined_RAPM_G$GPM_60 <- joined_RAPM_G$GF_60 - joined_RAPM_G$GA_60
joined_RAPM_C$CPM_60 <- joined_RAPM_C$CF_60 - joined_RAPM_C$CA_60

joined_RAPM <- inner_join(joined_RAPM_xG, joined_RAPM_G, by="Player")
joined_RAPM <- inner_join(joined_RAPM, joined_RAPM_C, by="Player")

joined_RAPM <- joined_RAPM %>%
  arrange(desc(xGPM_60))

joined_RAPM$Player = as.numeric(as.character(joined_RAPM$Player))

joined_RAPM <- joined_RAPM %>% rename(playerID = Player)
rm(defense_RAPM_xGA,defense_RAPM_GA,defense_RAPM_CA,joined_RAPM_G,joined_RAPM_C,joined_RAPM_xG,offense_RAPM_xGF,offense_RAPM_GF,offense_RAPM_CF,Binded_Coefficients_GF,Binded_Coefficients_CF,Binded_Coefficients_xGF,Cross_Validated_Results_GF,Cross_Validated_Results_CF,Cross_Validated_Results_xGF,RAPM_GF_coefficients,RAPM_CF_coefficients,RAPM_xGF_coefficients,RAPM_xGF,Run_RAPM_xGF,Run_RAPM_GF,Run_RAPM_CF,Sparse_RAPM_xGF)
rm(even_strength,GF60,power_play,shift_length,xGF60,Sparse_RAPM_GF,Sparse_RAPM_CF,RAPM_GF,RAPM_CF,CF60)

RAPM <- joined_RAPM

RAPM_even <- RAPM
RAPM_even <- RAPM_even %>%
  rename(RAPM_xGF = xGF_60,RAPM_GF = GF_60,RAPM_CF = CF_60,RAPM_xGA = xGA_60,RAPM_GA = GA_60,RAPM_CA = CA_60)

individual_even <- skater_individual(pbp,"even") %>%
  left_join(toi_5v5) %>%
  mutate(g_60 = (goals/toi)*60,prim_a_60 = (assists_prim/toi)*60,sec_a_60 = (assists_sec/toi)*60,ixg_60 = (ixg/toi)*60,icf_60 = (icf/toi)*60)
individual_even <- na.omit(individual_even)

onice_even <- skater_onice(pbp,"even") %>%
  left_join(toi_5v5) %>%
  mutate(gf_perc = gf/(gf+ga),xgf_perc = xgf/(xgf+xga),cf_perc = cf/(cf+ca),gf_60 = (gf/toi)*60,ga_60 = (ga/toi)*60,xgf_60 = (xgf/toi)*60,xga_60 = (xga/toi)*60,cf_60 = (cf/toi)*60,ca_60 = (ca/toi)*60) %>%
  select(-toi)
onice_even <- na.omit(onice_even)

even_summary <- RAPM_even %>% left_join(individual_even,by="playerID") %>% left_join(onice_even,by="playerID")
even_summary <- even_summary %>%
  filter(toi > toi_threshold_even) %>%
  mutate(EVO = ((((0.75*goals) + (0.7*assists_prim) + (0.55*assists_sec) + (0.075*isog) + (0.1*ixg) + (0.05*cf) + (0.1*xgf) + (0.15*gf)) / gp) + ((RAPM_xGF + (RAPM_GF/5))/2))/2,
         EVD = (RAPM_xGA + (RAPM_GA/5))/2)
even_summary <- even_summary[!is.na(even_summary$EVO),]
even_summary <- even_summary[!is.na(even_summary$EVD),]
even_summary <- even_summary %>%
  select(playerID,EVO,EVD) %>% arrange(desc(EVO))
rm(individual_even,onice_even,toi_5v5,RAPM_even)

pbp <- plays

even_strength <- c("3v3", "4v4", "5v5")
power_play <- c("5v4","5v3","4v3","6v4","6v3")
penalty_kill <- c("4v5","3v5","3v4","4v6","3v6")

pbp <- pbp %>%
  filter(period < 5)

pbp$home_on_1 <- ifelse(is.na(pbp$home_on_1), "MISSING", pbp$home_on_1)
pbp$home_on_2 <- ifelse(is.na(pbp$home_on_2), "MISSING", pbp$home_on_2)
pbp$home_on_3 <- ifelse(is.na(pbp$home_on_3), "MISSING", pbp$home_on_3)
pbp$home_on_4 <- ifelse(is.na(pbp$home_on_4), "MISSING", pbp$home_on_4)
pbp$home_on_5 <- ifelse(is.na(pbp$home_on_5), "MISSING", pbp$home_on_5)
pbp$home_on_6 <- ifelse(is.na(pbp$home_on_6), "MISSING", pbp$home_on_6)
pbp$away_on_1 <- ifelse(is.na(pbp$away_on_1), "MISSING", pbp$away_on_1)
pbp$away_on_2 <- ifelse(is.na(pbp$away_on_2), "MISSING", pbp$away_on_2)
pbp$away_on_3 <- ifelse(is.na(pbp$away_on_3), "MISSING", pbp$away_on_3)
pbp$away_on_4 <- ifelse(is.na(pbp$away_on_4), "MISSING", pbp$away_on_4)
pbp$away_on_5 <- ifelse(is.na(pbp$away_on_5), "MISSING", pbp$away_on_5)
pbp$away_on_6 <- ifelse(is.na(pbp$away_on_6), "MISSING", pbp$away_on_6)

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
pbp$home_xGF <- ifelse(pbp$event_team_name==pbp$home_team, pbp$xg, 0)
pbp$away_xGF <- ifelse(pbp$event_team_name!=pbp$home_team, pbp$xg, 0)
pbp$home_GF <- ifelse(pbp$event_type=="GOAL" & pbp$event_team_name==pbp$home_team, 1, 0)
pbp$away_GF <- ifelse(pbp$event_type=="GOAL" & pbp$event_team_name==pbp$away_team, 1, 0)
pbp$home_CF <- ifelse(pbp$event_type %in% c("GOAL","SHOT_ON_GOAL","MISSED_SHOT","BLOCKED_SHOT") & pbp$event_team_name==pbp$home_team, 1, 0)
pbp$away_CF <- ifelse(pbp$event_type %in% c("GOAL","SHOT_ON_GOAL","MISSED_SHOT","BLOCKED_SHOT") & pbp$event_team_name==pbp$away_team, 1, 0)
pbp$tied <- ifelse(pbp$home_score==pbp$away_score, 1, 0)
pbp$home_lead_1 <- ifelse(pbp$home_score-pbp$away_score==1, 1, 0)
pbp$home_lead_2 <- ifelse(pbp$home_score-pbp$away_score==2, 1, 0)
pbp$home_lead_3 <- ifelse(pbp$home_score-pbp$away_score>=3, 1, 0)
pbp$away_lead_1 <- ifelse(pbp$home_score-pbp$away_score==(-1), 1, 0)
pbp$away_lead_2 <- ifelse(pbp$home_score-pbp$away_score==(-2), 1, 0)
pbp$away_lead_3 <- ifelse(pbp$home_score-pbp$away_score<=(-3), 1, 0)
pbp$Five <- ifelse(pbp$strength_state=="5v5", 1, 0)
pbp$Four <- ifelse(pbp$strength_state=="4v4", 1, 0)
pbp$Three <- ifelse(pbp$strength_state=="3v3", 1, 0)
pbp$home_xGF[is.na(pbp$home_xGF)] <- 0
pbp$away_xGF[is.na(pbp$away_xGF)] <- 0
pbp$event_length[is.na(pbp$event_length)] <- 0

pbp_ev <- pbp %>%
  filter(strength_state %in% power_play)

grouped_shifts <- pbp_ev %>%
  group_by(game_id, shift_change_index, period, game_score_state, home_on_1, home_on_2, home_on_3, home_on_4, home_on_5, home_on_6, 
           away_on_1, away_on_2, away_on_3, away_on_4, away_on_5, away_on_6) %>%
  summarise(shift_length = sum(event_length), homexGF = sum(home_xGF), awayxGF = sum(away_xGF),
            homeGF = sum(home_GF), awayGF = sum(away_GF), homeCF = sum(home_CF), awayCF = sum(away_CF),
            Home_Up_1 = max(home_lead_1), Home_Up_2 = max(home_lead_2), Home_Up_3 = max(home_lead_3),
            Away_Up_1 = max(away_lead_1), Away_Up_2 = max(away_lead_2), Away_Up_3 = max(away_lead_3), Tied = max(tied), 
            State_5v5 = max(Five), State_4v4 = max(Four), State_3v3 = max(Three)) %>%
  filter(shift_length > 0)

home_as_off <- grouped_shifts %>%
  rename(offense_1 = home_on_1, offense_2 = home_on_2, offense_3 = home_on_3, offense_4 = home_on_4, offense_5 = home_on_5, offense_6 = home_on_6,
         defense_1 = away_on_1, defense_2 = away_on_2, defense_3 = away_on_3, defense_4 = away_on_4, defense_5 = away_on_5, defense_6 = away_on_6,
         Up_1 = Home_Up_1, Up_2 = Home_Up_2, Up_3 = Home_Up_3, Down_1 = Away_Up_1, Down_2 = Away_Up_2, Down_3 = Away_Up_3, xGF = homexGF, GF = homeGF, CF = homeCF) %>%
  select(game_id, shift_change_index, period, game_score_state,
         offense_1, offense_2, offense_3, offense_4, offense_5, offense_6, 
         defense_1, defense_2, defense_3, defense_4, defense_5, defense_6, 
         xGF, GF, CF, shift_length, shift_change_index, Tied, State_5v5, State_4v4, State_3v3, 
         Up_1, Up_2, Up_3, Down_1, Down_2, Down_3) %>%
  mutate(xGF_60 = xGF*3600/shift_length, GF_60 = GF*3600/shift_length, CF_60 = CF*3600/shift_length, is_home = 1)

away_as_off <- grouped_shifts %>%
  rename(offense_1 = away_on_1, offense_2 = away_on_2, offense_3 = away_on_3, offense_4 = away_on_4, offense_5 = away_on_5, offense_6 = away_on_6,
         defense_1 = home_on_1, defense_2 = home_on_2, defense_3 = home_on_3, defense_4 = home_on_4, defense_5 = home_on_5, defense_6 = home_on_6,
         Up_1 = Away_Up_1, Up_2 = Away_Up_2, Up_3 = Away_Up_3, Down_1 = Home_Up_1, Down_2 = Home_Up_2, Down_3 = Home_Up_3, xGF = awayxGF, GF = awayGF, CF = awayCF) %>%
  select(game_id, shift_change_index, period, game_score_state,
         offense_1, offense_2, offense_3, offense_4, offense_5, offense_6, 
         defense_1, defense_2, defense_3, defense_4, defense_5, defense_6, 
         xGF, GF, CF, shift_length, shift_change_index, Tied, State_5v5, State_4v4, State_3v3, 
         Up_1, Up_2, Up_3, Down_1, Down_2, Down_3) %>%
  mutate(xGF_60 = xGF*3600/shift_length, GF_60 = GF*3600/shift_length, CF_60 = CF*3600/shift_length, is_home = 0)

shifts_combined <- full_join(home_as_off, away_as_off)
rm(pbp_ev,home_as_off,away_as_off,grouped_shifts)

shifts_subset = subset(shifts_combined, select = c(offense_1:offense_6,defense_1:defense_6,shift_length,State_4v4,State_3v3,Up_1:Down_3,xGF_60,GF_60,CF_60,is_home))

shifts_combined_dummies_off <- dummy_cols(shifts_subset, select_columns = c("offense_1","offense_2","offense_3","offense_4","offense_5","offense_6"))
shifts_combined_dummies_def <- dummy_cols(shifts_subset, select_columns = c("defense_1","defense_2","defense_3","defense_4","defense_5","defense_6"))
shifts_combined_dummies_off = subset(shifts_combined_dummies_off, select = -c(offense_1,offense_2,offense_3,offense_4,offense_5,offense_6,defense_1,defense_2,defense_3,defense_4,defense_5,defense_6))
shifts_combined_dummies_def = subset(shifts_combined_dummies_def, select = -c(offense_1,offense_2,offense_3,offense_4,offense_5,offense_6,defense_1,defense_2,defense_3,defense_4,defense_5,defense_6,shift_length,State_4v4,State_3v3,Up_1:Down_3,xGF_60,GF_60,CF_60,is_home))
shifts_combined_dummies <- cbind(shifts_combined_dummies_off, shifts_combined_dummies_def)
rm(shifts_subset,shifts_combined_dummies_off,shifts_combined_dummies_def,shifts_combined)

colnames(shifts_combined_dummies) = gsub("offense_1", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("offense_2", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("offense_3", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("offense_4", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("offense_5", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("offense_6", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_1", "defense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_2", "defense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_3", "defense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_4", "defense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_5", "defense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_6", "defense_", colnames(shifts_combined_dummies))

shifts_combined_dummies <- as.data.frame(lapply(split.default(shifts_combined_dummies, names(shifts_combined_dummies)), function(x) Reduce(`+`, x)))

shifts_combined_dummies <- shifts_combined_dummies %>% select(-contains("Goalie"))
shifts_combined_dummies <- shifts_combined_dummies %>% select(-contains("Missing"))

xGF60 <- as.numeric(c(shifts_combined_dummies$xGF_60))
GF60 <- as.numeric(c(shifts_combined_dummies$GF_60))
CF60 <- as.numeric(c(shifts_combined_dummies$CF_60))
shift_length <- as.numeric(c(shifts_combined_dummies$shift_length))
subsetted_dummies = subset(shifts_combined_dummies, select = -c(shift_length, xGF_60, GF_60, CF_60))

RAPM_xGF <- as.matrix(subsetted_dummies)
Sparse_RAPM_xGF <- Matrix(RAPM_xGF, sparse = TRUE)
rm(RAPM_xGF)

RAPM_GF <- as.matrix(subsetted_dummies)
Sparse_RAPM_GF <- Matrix(RAPM_GF, sparse = TRUE)
rm(RAPM_GF)

RAPM_CF <- as.matrix(subsetted_dummies)
Sparse_RAPM_CF <- Matrix(RAPM_CF, sparse = TRUE)
rm(RAPM_CF)

rm(shifts_combined_dummies,subsetted_dummies)
Cross_Validated_Results_xGF <- cv.glmnet(x=Sparse_RAPM_xGF, y=xGF60, weights=shift_length, alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)
Cross_Validated_Results_GF <- cv.glmnet(x=Sparse_RAPM_GF, y=GF60, weights=shift_length, alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)
Cross_Validated_Results_CF <- cv.glmnet(x=Sparse_RAPM_CF, y=CF60, weights=shift_length, alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)

Run_RAPM_xGF <- glmnet(x=Sparse_RAPM_xGF, y=xGF60, weights=shift_length, lambda = Cross_Validated_Results_xGF[["lambda.min"]], alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)
Run_RAPM_GF <- glmnet(x=Sparse_RAPM_GF, y=GF60, weights=shift_length, lambda = Cross_Validated_Results_xGF[["lambda.min"]], alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)
Run_RAPM_CF <- glmnet(x=Sparse_RAPM_CF, y=CF60, weights=shift_length, lambda = Cross_Validated_Results_xGF[["lambda.min"]], alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)

RAPM_xGF_coefficients <- as.data.frame(as.matrix(coef(Run_RAPM_xGF)))
RAPM_GF_coefficients <- as.data.frame(as.matrix(coef(Run_RAPM_GF)))
RAPM_CF_coefficients <- as.data.frame(as.matrix(coef(Run_RAPM_CF)))


Binded_Coefficients_xGF <- cbind(rownames(RAPM_xGF_coefficients), RAPM_xGF_coefficients) %>%
  rename(Player = `rownames(RAPM_xGF_coefficients)`, xGF_60 = s0)
Binded_Coefficients_GF <- cbind(rownames(RAPM_GF_coefficients), RAPM_GF_coefficients) %>%
  rename(Player = `rownames(RAPM_GF_coefficients)`, GF_60 = s0)
Binded_Coefficients_CF <- cbind(rownames(RAPM_CF_coefficients), RAPM_CF_coefficients) %>%
  rename(Player = `rownames(RAPM_CF_coefficients)`, CF_60 = s0)

offense_RAPM_xGF <- Binded_Coefficients_xGF %>%
  filter(grepl("offense", Binded_Coefficients_xGF$Player))
offense_RAPM_GF <- Binded_Coefficients_GF %>%
  filter(grepl("offense", Binded_Coefficients_GF$Player))
offense_RAPM_CF <- Binded_Coefficients_CF %>%
  filter(grepl("offense", Binded_Coefficients_CF$Player))

offense_RAPM_xGF$Player = str_replace_all(offense_RAPM_xGF$Player, "offense__", "")
offense_RAPM_GF$Player = str_replace_all(offense_RAPM_GF$Player, "offense__", "")
offense_RAPM_CF$Player = str_replace_all(offense_RAPM_CF$Player, "offense__", "")

defense_RAPM_xGA <- Binded_Coefficients_xGF %>%
  filter(grepl("defense", Binded_Coefficients_xGF$Player)) %>%
  rename(xGA_60 = xGF_60)
defense_RAPM_GA <- Binded_Coefficients_GF %>%
  filter(grepl("defense", Binded_Coefficients_GF$Player)) %>%
  rename(GA_60 = GF_60)
defense_RAPM_CA <- Binded_Coefficients_CF %>%
  filter(grepl("defense", Binded_Coefficients_CF$Player)) %>%
  rename(CA_60 = CF_60)

defense_RAPM_xGA$Player = str_replace_all(defense_RAPM_xGA$Player, "defense__", "")
defense_RAPM_GA$Player = str_replace_all(defense_RAPM_GA$Player, "defense__", "")
defense_RAPM_CA$Player = str_replace_all(defense_RAPM_CA$Player, "defense__", "")

joined_RAPM_xG <- inner_join(offense_RAPM_xGF, defense_RAPM_xGA, by="Player")
joined_RAPM_G <- inner_join(offense_RAPM_GF, defense_RAPM_GA, by="Player")
joined_RAPM_C <- inner_join(offense_RAPM_CF, defense_RAPM_CA, by="Player")

joined_RAPM_xG$xGPM_60 <- joined_RAPM_xG$xGF_60 - joined_RAPM_xG$xGA_60
joined_RAPM_G$GPM_60 <- joined_RAPM_G$GF_60 - joined_RAPM_G$GA_60
joined_RAPM_C$CPM_60 <- joined_RAPM_C$CF_60 - joined_RAPM_C$CA_60

joined_RAPM <- inner_join(joined_RAPM_xG, joined_RAPM_G, by="Player")
joined_RAPM <- inner_join(joined_RAPM, joined_RAPM_C, by="Player")

joined_RAPM <- joined_RAPM %>%
  arrange(desc(xGPM_60))

joined_RAPM$Player = as.numeric(as.character(joined_RAPM$Player))

joined_RAPM <- joined_RAPM %>% rename(playerID = Player)
rm(defense_RAPM_xGA,defense_RAPM_GA,defense_RAPM_CA,joined_RAPM_G,joined_RAPM_C,joined_RAPM_xG,offense_RAPM_xGF,offense_RAPM_GF,offense_RAPM_CF,Binded_Coefficients_GF,Binded_Coefficients_CF,Binded_Coefficients_xGF,Cross_Validated_Results_GF,Cross_Validated_Results_CF,Cross_Validated_Results_xGF,RAPM_GF_coefficients,RAPM_CF_coefficients,RAPM_xGF_coefficients,RAPM_xGF,Run_RAPM_xGF,Run_RAPM_GF,Run_RAPM_CF,Sparse_RAPM_xGF)
rm(even_strength,GF60,power_play,shift_length,xGF60,Sparse_RAPM_GF,Sparse_RAPM_CF,RAPM_GF,RAPM_CF,CF60)

RAPM <- joined_RAPM

RAPM_PP <- RAPM
RAPM_PP <- RAPM_PP %>%
  rename(RAPM_xGF = xGF_60,RAPM_GF = GF_60,RAPM_CF = CF_60,RAPM_xGA = xGA_60,RAPM_GA = GA_60,RAPM_CA = CA_60)

individual_pp <- skater_individual(pbp,"pp") %>%
  left_join(toi_pp) %>%
  mutate(g_60 = (goals/toi)*60,prim_a_60 = (assists_prim/toi)*60, sec_a_60 = (assists_sec/toi)*60,isog_60 = (isog/toi)*60,ixg_60 = (ixg/toi)*60,icf_60 = (icf/toi)*60)
individual_pp <- na.omit(individual_pp)

onice_pp <- skater_onice(pbp,"pp") %>%
  left_join(toi_pp) %>%
  mutate(gf_perc = gf/(gf+ga),xgf_perc = xgf/(xgf+xga),cf_perc = cf/(cf+ca),gf_60 = (gf/toi)*60,ga_60 = (ga/toi)*60,xgf_60 = (xgf/toi)*60,xga_60 = (xga/toi)*60,cf_60 = (cf/toi)*60,ca_60 = (ca/toi)*60) %>%
  select(-toi)
onice_pp <- na.omit(onice_pp)

pp_summary <- RAPM_PP %>% left_join(individual_pp,by="playerID") %>% left_join(onice_pp,by="playerID")
pp_summary <- pp_summary %>% filter(toi>toi_threshold_spec)
pp_summary <- pp_summary %>%
  mutate(PPO = ((((0.75*g_60) + (0.7*prim_a_60) + (0.55*sec_a_60) + (0.075*isog_60) + (0.1*ixg_60) + (0.05*cf_60) + (0.1*xgf_60) + (0.15*gf_60)) / gp) + ((RAPM_xGF + (RAPM_GF/5))/2))/2)
pp_summary <- pp_summary[!is.na(pp_summary$PPO),]
pp_summary <- pp_summary %>%
  select(playerID,PPO) %>% arrange(desc(PPO))
rm(individual_pp,onice_pp,RAPM_PP,toi_pp)

pbp <- plays

even_strength <- c("3v3", "4v4", "5v5")
power_play <- c("5v4","5v3","4v3","6v4","6v3")
penalty_kill <- c("4v5","3v5","3v4","4v6","3v6")

pbp <- pbp %>%
  filter(period < 5)

pbp$home_on_1 <- ifelse(is.na(pbp$home_on_1), "MISSING", pbp$home_on_1)
pbp$home_on_2 <- ifelse(is.na(pbp$home_on_2), "MISSING", pbp$home_on_2)
pbp$home_on_3 <- ifelse(is.na(pbp$home_on_3), "MISSING", pbp$home_on_3)
pbp$home_on_4 <- ifelse(is.na(pbp$home_on_4), "MISSING", pbp$home_on_4)
pbp$home_on_5 <- ifelse(is.na(pbp$home_on_5), "MISSING", pbp$home_on_5)
pbp$home_on_6 <- ifelse(is.na(pbp$home_on_6), "MISSING", pbp$home_on_6)
pbp$away_on_1 <- ifelse(is.na(pbp$away_on_1), "MISSING", pbp$away_on_1)
pbp$away_on_2 <- ifelse(is.na(pbp$away_on_2), "MISSING", pbp$away_on_2)
pbp$away_on_3 <- ifelse(is.na(pbp$away_on_3), "MISSING", pbp$away_on_3)
pbp$away_on_4 <- ifelse(is.na(pbp$away_on_4), "MISSING", pbp$away_on_4)
pbp$away_on_5 <- ifelse(is.na(pbp$away_on_5), "MISSING", pbp$away_on_5)
pbp$away_on_6 <- ifelse(is.na(pbp$away_on_6), "MISSING", pbp$away_on_6)

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
pbp$home_xGF <- ifelse(pbp$event_team_name==pbp$home_team, pbp$xg, 0)
pbp$away_xGF <- ifelse(pbp$event_team_name!=pbp$home_team, pbp$xg, 0)
pbp$home_GF <- ifelse(pbp$event_type=="GOAL" & pbp$event_team_name==pbp$home_team, 1, 0)
pbp$away_GF <- ifelse(pbp$event_type=="GOAL" & pbp$event_team_name==pbp$away_team, 1, 0)
pbp$home_CF <- ifelse(pbp$event_type %in% c("GOAL","SHOT_ON_GOAL","MISSED_SHOT","BLOCKED_SHOT") & pbp$event_team_name==pbp$home_team, 1, 0)
pbp$away_CF <- ifelse(pbp$event_type %in% c("GOAL","SHOT_ON_GOAL","MISSED_SHOT","BLOCKED_SHOT") & pbp$event_team_name==pbp$away_team, 1, 0)
pbp$tied <- ifelse(pbp$home_score==pbp$away_score, 1, 0)
pbp$home_lead_1 <- ifelse(pbp$home_score-pbp$away_score==1, 1, 0)
pbp$home_lead_2 <- ifelse(pbp$home_score-pbp$away_score==2, 1, 0)
pbp$home_lead_3 <- ifelse(pbp$home_score-pbp$away_score>=3, 1, 0)
pbp$away_lead_1 <- ifelse(pbp$home_score-pbp$away_score==(-1), 1, 0)
pbp$away_lead_2 <- ifelse(pbp$home_score-pbp$away_score==(-2), 1, 0)
pbp$away_lead_3 <- ifelse(pbp$home_score-pbp$away_score<=(-3), 1, 0)
pbp$Five <- ifelse(pbp$strength_state=="5v5", 1, 0)
pbp$Four <- ifelse(pbp$strength_state=="4v4", 1, 0)
pbp$Three <- ifelse(pbp$strength_state=="3v3", 1, 0)
pbp$home_xGF[is.na(pbp$home_xGF)] <- 0
pbp$away_xGF[is.na(pbp$away_xGF)] <- 0
pbp$event_length[is.na(pbp$event_length)] <- 0

pbp_ev <- pbp %>%
  filter(strength_state %in% penalty_kill)

grouped_shifts <- pbp_ev %>%
  group_by(game_id, shift_change_index, period, game_score_state, home_on_1, home_on_2, home_on_3, home_on_4, home_on_5, home_on_6, 
           away_on_1, away_on_2, away_on_3, away_on_4, away_on_5, away_on_6) %>%
  summarise(shift_length = sum(event_length), homexGF = sum(home_xGF), awayxGF = sum(away_xGF),
            homeGF = sum(home_GF), awayGF = sum(away_GF), homeCF = sum(home_CF), awayCF = sum(away_CF),
            Home_Up_1 = max(home_lead_1), Home_Up_2 = max(home_lead_2), Home_Up_3 = max(home_lead_3),
            Away_Up_1 = max(away_lead_1), Away_Up_2 = max(away_lead_2), Away_Up_3 = max(away_lead_3), Tied = max(tied), 
            State_5v5 = max(Five), State_4v4 = max(Four), State_3v3 = max(Three)) %>%
  filter(shift_length > 0)

home_as_off <- grouped_shifts %>%
  rename(offense_1 = home_on_1, offense_2 = home_on_2, offense_3 = home_on_3, offense_4 = home_on_4, offense_5 = home_on_5, offense_6 = home_on_6,
         defense_1 = away_on_1, defense_2 = away_on_2, defense_3 = away_on_3, defense_4 = away_on_4, defense_5 = away_on_5, defense_6 = away_on_6,
         Up_1 = Home_Up_1, Up_2 = Home_Up_2, Up_3 = Home_Up_3, Down_1 = Away_Up_1, Down_2 = Away_Up_2, Down_3 = Away_Up_3, xGF = homexGF, GF = homeGF, CF = homeCF) %>%
  select(game_id, shift_change_index, period, game_score_state,
         offense_1, offense_2, offense_3, offense_4, offense_5, offense_6, 
         defense_1, defense_2, defense_3, defense_4, defense_5, defense_6, 
         xGF, GF, CF, shift_length, shift_change_index, Tied, State_5v5, State_4v4, State_3v3, 
         Up_1, Up_2, Up_3, Down_1, Down_2, Down_3) %>%
  mutate(xGF_60 = xGF*3600/shift_length, GF_60 = GF*3600/shift_length, CF_60 = CF*3600/shift_length, is_home = 1)

away_as_off <- grouped_shifts %>%
  rename(offense_1 = away_on_1, offense_2 = away_on_2, offense_3 = away_on_3, offense_4 = away_on_4, offense_5 = away_on_5, offense_6 = away_on_6,
         defense_1 = home_on_1, defense_2 = home_on_2, defense_3 = home_on_3, defense_4 = home_on_4, defense_5 = home_on_5, defense_6 = home_on_6,
         Up_1 = Away_Up_1, Up_2 = Away_Up_2, Up_3 = Away_Up_3, Down_1 = Home_Up_1, Down_2 = Home_Up_2, Down_3 = Home_Up_3, xGF = awayxGF, GF = awayGF, CF = awayCF) %>%
  select(game_id, shift_change_index, period, game_score_state,
         offense_1, offense_2, offense_3, offense_4, offense_5, offense_6, 
         defense_1, defense_2, defense_3, defense_4, defense_5, defense_6, 
         xGF, GF, CF, shift_length, shift_change_index, Tied, State_5v5, State_4v4, State_3v3, 
         Up_1, Up_2, Up_3, Down_1, Down_2, Down_3) %>%
  mutate(xGF_60 = xGF*3600/shift_length, GF_60 = GF*3600/shift_length, CF_60 = CF*3600/shift_length, is_home = 0)

shifts_combined <- full_join(home_as_off, away_as_off)
rm(pbp_ev,home_as_off,away_as_off,grouped_shifts)

shifts_subset = subset(shifts_combined, select = c(offense_1:offense_6,defense_1:defense_6,shift_length,State_4v4,State_3v3,Up_1:Down_3,xGF_60,GF_60,CF_60,is_home))

shifts_combined_dummies_off <- dummy_cols(shifts_subset, select_columns = c("offense_1","offense_2","offense_3","offense_4","offense_5","offense_6"))
shifts_combined_dummies_def <- dummy_cols(shifts_subset, select_columns = c("defense_1","defense_2","defense_3","defense_4","defense_5","defense_6"))
shifts_combined_dummies_off = subset(shifts_combined_dummies_off, select = -c(offense_1,offense_2,offense_3,offense_4,offense_5,offense_6,defense_1,defense_2,defense_3,defense_4,defense_5,defense_6))
shifts_combined_dummies_def = subset(shifts_combined_dummies_def, select = -c(offense_1,offense_2,offense_3,offense_4,offense_5,offense_6,defense_1,defense_2,defense_3,defense_4,defense_5,defense_6,shift_length,State_4v4,State_3v3,Up_1:Down_3,xGF_60,GF_60,CF_60,is_home))
shifts_combined_dummies <- cbind(shifts_combined_dummies_off, shifts_combined_dummies_def)
rm(shifts_subset,shifts_combined_dummies_off,shifts_combined_dummies_def,shifts_combined)

colnames(shifts_combined_dummies) = gsub("offense_1", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("offense_2", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("offense_3", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("offense_4", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("offense_5", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("offense_6", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_1", "defense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_2", "defense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_3", "defense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_4", "defense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_5", "defense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_6", "defense_", colnames(shifts_combined_dummies))

shifts_combined_dummies <- as.data.frame(lapply(split.default(shifts_combined_dummies, names(shifts_combined_dummies)), function(x) Reduce(`+`, x)))

shifts_combined_dummies <- shifts_combined_dummies %>% select(-contains("Goalie"))
shifts_combined_dummies <- shifts_combined_dummies %>% select(-contains("Missing"))

xGF60 <- as.numeric(c(shifts_combined_dummies$xGF_60))
GF60 <- as.numeric(c(shifts_combined_dummies$GF_60))
CF60 <- as.numeric(c(shifts_combined_dummies$CF_60))
shift_length <- as.numeric(c(shifts_combined_dummies$shift_length))
subsetted_dummies = subset(shifts_combined_dummies, select = -c(shift_length, xGF_60, GF_60, CF_60))

RAPM_xGF <- as.matrix(subsetted_dummies)
Sparse_RAPM_xGF <- Matrix(RAPM_xGF, sparse = TRUE)
rm(RAPM_xGF)

RAPM_GF <- as.matrix(subsetted_dummies)
Sparse_RAPM_GF <- Matrix(RAPM_GF, sparse = TRUE)
rm(RAPM_GF)

RAPM_CF <- as.matrix(subsetted_dummies)
Sparse_RAPM_CF <- Matrix(RAPM_CF, sparse = TRUE)
rm(RAPM_CF)

rm(shifts_combined_dummies,subsetted_dummies)
Cross_Validated_Results_xGF <- cv.glmnet(x=Sparse_RAPM_xGF, y=xGF60, weights=shift_length, alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)
Cross_Validated_Results_GF <- cv.glmnet(x=Sparse_RAPM_GF, y=GF60, weights=shift_length, alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)
Cross_Validated_Results_CF <- cv.glmnet(x=Sparse_RAPM_CF, y=CF60, weights=shift_length, alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)

Run_RAPM_xGF <- glmnet(x=Sparse_RAPM_xGF, y=xGF60, weights=shift_length, lambda = Cross_Validated_Results_xGF[["lambda.min"]], alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)
Run_RAPM_GF <- glmnet(x=Sparse_RAPM_GF, y=GF60, weights=shift_length, lambda = Cross_Validated_Results_xGF[["lambda.min"]], alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)
Run_RAPM_CF <- glmnet(x=Sparse_RAPM_CF, y=CF60, weights=shift_length, lambda = Cross_Validated_Results_xGF[["lambda.min"]], alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)

RAPM_xGF_coefficients <- as.data.frame(as.matrix(coef(Run_RAPM_xGF)))
RAPM_GF_coefficients <- as.data.frame(as.matrix(coef(Run_RAPM_GF)))
RAPM_CF_coefficients <- as.data.frame(as.matrix(coef(Run_RAPM_CF)))

Binded_Coefficients_xGF <- cbind(rownames(RAPM_xGF_coefficients), RAPM_xGF_coefficients) %>%
  rename(Player = `rownames(RAPM_xGF_coefficients)`, xGF_60 = s0)
Binded_Coefficients_GF <- cbind(rownames(RAPM_GF_coefficients), RAPM_GF_coefficients) %>%
  rename(Player = `rownames(RAPM_GF_coefficients)`, GF_60 = s0)
Binded_Coefficients_CF <- cbind(rownames(RAPM_CF_coefficients), RAPM_CF_coefficients) %>%
  rename(Player = `rownames(RAPM_CF_coefficients)`, CF_60 = s0)

offense_RAPM_xGF <- Binded_Coefficients_xGF %>%
  filter(grepl("offense", Binded_Coefficients_xGF$Player))
offense_RAPM_GF <- Binded_Coefficients_GF %>%
  filter(grepl("offense", Binded_Coefficients_GF$Player))
offense_RAPM_CF <- Binded_Coefficients_CF %>%
  filter(grepl("offense", Binded_Coefficients_CF$Player))

offense_RAPM_xGF$Player = str_replace_all(offense_RAPM_xGF$Player, "offense__", "")
offense_RAPM_GF$Player = str_replace_all(offense_RAPM_GF$Player, "offense__", "")
offense_RAPM_CF$Player = str_replace_all(offense_RAPM_CF$Player, "offense__", "")

defense_RAPM_xGA <- Binded_Coefficients_xGF %>%
  filter(grepl("defense", Binded_Coefficients_xGF$Player)) %>%
  rename(xGA_60 = xGF_60)
defense_RAPM_GA <- Binded_Coefficients_GF %>%
  filter(grepl("defense", Binded_Coefficients_GF$Player)) %>%
  rename(GA_60 = GF_60)
defense_RAPM_CA <- Binded_Coefficients_CF %>%
  filter(grepl("defense", Binded_Coefficients_CF$Player)) %>%
  rename(CA_60 = CF_60)

defense_RAPM_xGA$Player = str_replace_all(defense_RAPM_xGA$Player, "defense__", "")
defense_RAPM_GA$Player = str_replace_all(defense_RAPM_GA$Player, "defense__", "")
defense_RAPM_CA$Player = str_replace_all(defense_RAPM_CA$Player, "defense__", "")

joined_RAPM_xG <- inner_join(offense_RAPM_xGF, defense_RAPM_xGA, by="Player")
joined_RAPM_G <- inner_join(offense_RAPM_GF, defense_RAPM_GA, by="Player")
joined_RAPM_C <- inner_join(offense_RAPM_CF, defense_RAPM_CA, by="Player")

joined_RAPM_xG$xGPM_60 <- joined_RAPM_xG$xGF_60 - joined_RAPM_xG$xGA_60
joined_RAPM_G$GPM_60 <- joined_RAPM_G$GF_60 - joined_RAPM_G$GA_60
joined_RAPM_C$CPM_60 <- joined_RAPM_C$CF_60 - joined_RAPM_C$CA_60

joined_RAPM <- inner_join(joined_RAPM_xG, joined_RAPM_G, by="Player")
joined_RAPM <- inner_join(joined_RAPM, joined_RAPM_C, by="Player")

joined_RAPM <- joined_RAPM %>%
  arrange(desc(xGPM_60))

joined_RAPM$Player = as.numeric(as.character(joined_RAPM$Player))

joined_RAPM <- joined_RAPM %>% rename(playerID = Player)
rm(defense_RAPM_xGA,defense_RAPM_GA,defense_RAPM_CA,joined_RAPM_G,joined_RAPM_C,joined_RAPM_xG,offense_RAPM_xGF,offense_RAPM_GF,offense_RAPM_CF,Binded_Coefficients_GF,Binded_Coefficients_CF,Binded_Coefficients_xGF,Cross_Validated_Results_GF,Cross_Validated_Results_CF,Cross_Validated_Results_xGF,RAPM_GF_coefficients,RAPM_CF_coefficients,RAPM_xGF_coefficients,RAPM_xGF,Run_RAPM_xGF,Run_RAPM_GF,Run_RAPM_CF,Sparse_RAPM_xGF)
rm(even_strength,GF60,power_play,shift_length,xGF60,Sparse_RAPM_GF,Sparse_RAPM_CF,RAPM_GF,RAPM_CF,CF60)

RAPM <- joined_RAPM

RAPM_PK <- RAPM
RAPM_PK <- RAPM_PK %>%
  rename(RAPM_xGF = xGF_60,RAPM_GF = GF_60,RAPM_CF = CF_60,RAPM_xGA = xGA_60,RAPM_GA = GA_60,RAPM_CA = CA_60)

individual_pk <- skater_individual(pbp,"pk") %>% select(playerID,gp)
individual_pk <- na.omit(individual_pk)

onice_pk <- skater_onice(pbp,"pp") %>%
  left_join(toi_pk) %>%
  mutate(ga_60 = (ga/toi)*60,xga_60 = (xga/toi)*60,ca_60 = (ca/toi)*60) %>%
  select(playerID,ga,xga,ca,fa,toi,ga_60,xga_60,ca_60)
onice_pk <- na.omit(onice_pk)

pk_summary <- RAPM_PK %>% left_join(individual_pk,by="playerID") %>% left_join(onice_pk,by="playerID")
pk_summary <- pk_summary %>% filter(toi>toi_threshold_spec)
pk_summary <- pk_summary %>%
  mutate(SHD = (RAPM_xGA + (RAPM_GA/5))/2)
pk_summary <- pk_summary[!is.na(pk_summary$SHD),]
pk_summary <- pk_summary %>%
  select(playerID,SHD) %>% arrange(desc(SHD))
rm(individual_pk,onice_pk,RAPM_PK,toi_pk)

pbp <- plays

finishing_summary <- player_summary %>%
  mutate(FIN = (gax/toi)*ixg) %>%
  select(playerID,FIN)

penalty_summary <- player_summary %>%
  mutate(pens_against = (pens_taken/toi),
         pens_for = (pens_drawn/toi),
         PEN = (pens_for - pens_against)) %>%
  select(playerID,PEN)

player_summary <- all_summary %>% 
  left_join(even_summary,by="playerID") %>%
  left_join(pp_summary,by="playerID") %>%
  left_join(pk_summary,by="playerID") %>%
  left_join(finishing_summary,by="playerID") %>%
  left_join(penalty_summary,by="playerID") %>%
  mutate(
    across(PPO:PEN, ~replace_na(.x,0))
  ) %>%
  mutate(EVO = EVO,EVD = (EVD*0.78),PPO = (PPO*0.28),SHD = (SHD*0.2),FIN = (FIN*1.025),PEN = (PEN*15),
         OVR = EVO - EVD + PPO - SHD + FIN + PEN) %>%
  relocate(OVR,EVO,EVD,PPO,SHD,FIN,PEN,game_score,.after = playerID) %>%
  arrange(desc(OVR)) %>%
  filter(toi > toi_threshold_even)
player_summary <- na.omit(player_summary)

playernames <- dbReadTable(con,"player_data")
ids <- playernames$playerID
ids_to_check <- player_summary$playerID
ids_to_add <- setdiff(ids_to_check,ids)
if(length(ids_to_add)>0){
  for(i in 1:nrow(ids_to_add)){
    newplayer <- player(ids_to_add[i])
    playernames <- rbind(playernames,newplayer)
  }
  dbWriteTable(con, name = "player_data", value = playernames, row.names = FALSE, overwrite = TRUE)
}

player_summary <- playernames %>% left_join(player_summary, by="playerID")
player_summary <- na.omit(player_summary)
dbWriteTable(con, name = "player_stats_24_25", value = player_summary, row.names = FALSE, overwrite = TRUE)

pbp <- plays
goalie_games <- min(10,(length(unique(pbp$game_id))/16)/5)
goalie_summary <- goalie_stats(pbp,"all")
goalie_summary <- goalie_summary %>%
  filter(gp>goalie_games) %>%
  mutate(GOA = (gsax/shots_against)*xga/4+0.55) %>%
  relocate(GOA,.after = playerID) %>%
  arrange(desc(GOA))

playernames <- dbReadTable(con,"player_data")
ids <- playernames$playerID
ids_to_check <- goalie_summary$playerID
ids_to_add <- setdiff(ids_to_check,ids)
if(length(ids_to_add)>0){
  for(i in 1:nrow(ids_to_add)){
    newplayer <- player(ids_to_add[i])
    playernames <- rbind(playernames,newplayer)
  }
  dbWriteTable(con, name = "player_data", value = playernames, row.names = FALSE, overwrite = TRUE)
}
goalie_summary <- playernames %>% left_join(goalie_summary, by="playerID")
goalie_summary <- na.omit(goalie_summary)
dbWriteTable(con, name = "goalie_stats_24_25", value = goalie_summary, row.names = FALSE, overwrite = TRUE)

################################################################################

#### Player/Goalie Data - For Season Sim #######################################
pbp_24_25 <- dbReadTable(con,"pbp_24_25")
pbp_23_24 <- dbReadTable(con,"pbp_23_24")
plays <- bind_rows(pbp_24_25,pbp_23_24)
rm(pbp_24_25,pbp_23_24)
pbp <- plays

toi <- player_toi(pbp)
toi_all <- toi %>% select(playerID,toi_all) %>% rename(toi = toi_all)
toi_5v5 <- toi %>% select(playerID,toi_5v5) %>% rename(toi = toi_5v5)
toi_pp <- toi %>% select(playerID,toi_pp) %>% rename(toi = toi_pp)
toi_pk <- toi %>% select(playerID,toi_pk) %>% rename(toi = toi_pk)
toi_threshold_even <- min(250,(length(unique(pbp$game_id))/16)*10)
toi_threshold_spec <- min(75,(length(unique(pbp$game_id))/16)*2)

individual_all <- skater_individual(pbp,"all") %>%
  left_join(toi_all) %>%
  mutate(g_60 = (goals/toi)*60,prim_a_60 = (assists_prim/toi)*60,sec_a_60 = (assists_sec/toi)*60,ixg_60 = (ixg/toi)*60,icf_60 = (icf/toi)*60)
individual_all <- na.omit(individual_all)
onice_all <- skater_onice(pbp,"all") %>%
  left_join(toi_all) %>%
  mutate(gf_perc = gf/(gf+ga),xgf_perc = xgf/(xgf+xga),cf_perc = cf/(cf+ca),gf_60 = (gf/toi)*60,ga_60 = (ga/toi)*60,xgf_60 = (xgf/toi)*60,xga_60 = (xga/toi)*60,cf_60 = (cf/toi)*60,ca_60 = (ca/toi)*60) %>%
  select(-toi)
onice_all <- na.omit(onice_all)
player_summary <- individual_all %>% left_join(onice_all,by="playerID")
player_summary <- na.omit(player_summary)
all_summary <- player_summary %>%
  mutate(game_score = ((0.75*goals) + (0.7*assists_prim) + (0.55*assists_sec) + (0.075*isog) + (0.1*ixg) + (0.05*cf) + (0.1*xgf) + (0.15*gf) - (0.05*ca) - (0.1*xga) - (0.15*xga)) / gp)
rm(individual_all,onice_all)

even_strength <- c("3v3", "4v4", "5v5")
power_play <- c("5v4","5v3","4v3","6v4","6v3")
penalty_kill <- c("4v5","3v5","3v4","4v6","3v6")

pbp <- pbp %>%
  filter(period < 5)

pbp$home_on_1 <- ifelse(is.na(pbp$home_on_1), "MISSING", pbp$home_on_1)
pbp$home_on_2 <- ifelse(is.na(pbp$home_on_2), "MISSING", pbp$home_on_2)
pbp$home_on_3 <- ifelse(is.na(pbp$home_on_3), "MISSING", pbp$home_on_3)
pbp$home_on_4 <- ifelse(is.na(pbp$home_on_4), "MISSING", pbp$home_on_4)
pbp$home_on_5 <- ifelse(is.na(pbp$home_on_5), "MISSING", pbp$home_on_5)
pbp$home_on_6 <- ifelse(is.na(pbp$home_on_6), "MISSING", pbp$home_on_6)
pbp$away_on_1 <- ifelse(is.na(pbp$away_on_1), "MISSING", pbp$away_on_1)
pbp$away_on_2 <- ifelse(is.na(pbp$away_on_2), "MISSING", pbp$away_on_2)
pbp$away_on_3 <- ifelse(is.na(pbp$away_on_3), "MISSING", pbp$away_on_3)
pbp$away_on_4 <- ifelse(is.na(pbp$away_on_4), "MISSING", pbp$away_on_4)
pbp$away_on_5 <- ifelse(is.na(pbp$away_on_5), "MISSING", pbp$away_on_5)
pbp$away_on_6 <- ifelse(is.na(pbp$away_on_6), "MISSING", pbp$away_on_6)

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
pbp$home_xGF <- ifelse(pbp$event_team_name==pbp$home_team, pbp$xg, 0)
pbp$away_xGF <- ifelse(pbp$event_team_name!=pbp$home_team, pbp$xg, 0)
pbp$home_GF <- ifelse(pbp$event_type=="GOAL" & pbp$event_team_name==pbp$home_team, 1, 0)
pbp$away_GF <- ifelse(pbp$event_type=="GOAL" & pbp$event_team_name==pbp$away_team, 1, 0)
pbp$home_CF <- ifelse(pbp$event_type %in% c("GOAL","SHOT_ON_GOAL","MISSED_SHOT","BLOCKED_SHOT") & pbp$event_team_name==pbp$home_team, 1, 0)
pbp$away_CF <- ifelse(pbp$event_type %in% c("GOAL","SHOT_ON_GOAL","MISSED_SHOT","BLOCKED_SHOT") & pbp$event_team_name==pbp$away_team, 1, 0)
pbp$tied <- ifelse(pbp$home_score==pbp$away_score, 1, 0)
pbp$home_lead_1 <- ifelse(pbp$home_score-pbp$away_score==1, 1, 0)
pbp$home_lead_2 <- ifelse(pbp$home_score-pbp$away_score==2, 1, 0)
pbp$home_lead_3 <- ifelse(pbp$home_score-pbp$away_score>=3, 1, 0)
pbp$away_lead_1 <- ifelse(pbp$home_score-pbp$away_score==(-1), 1, 0)
pbp$away_lead_2 <- ifelse(pbp$home_score-pbp$away_score==(-2), 1, 0)
pbp$away_lead_3 <- ifelse(pbp$home_score-pbp$away_score<=(-3), 1, 0)
pbp$Five <- ifelse(pbp$strength_state=="5v5", 1, 0)
pbp$Four <- ifelse(pbp$strength_state=="4v4", 1, 0)
pbp$Three <- ifelse(pbp$strength_state=="3v3", 1, 0)
pbp$home_xGF[is.na(pbp$home_xGF)] <- 0
pbp$away_xGF[is.na(pbp$away_xGF)] <- 0
pbp$event_length[is.na(pbp$event_length)] <- 0

pbp_ev <- pbp %>%
  filter(strength_state %in% even_strength)

grouped_shifts <- pbp_ev %>%
  group_by(game_id, shift_change_index, period, game_score_state, home_on_1, home_on_2, home_on_3, home_on_4, home_on_5, home_on_6, 
           away_on_1, away_on_2, away_on_3, away_on_4, away_on_5, away_on_6) %>%
  summarise(shift_length = sum(event_length), homexGF = sum(home_xGF), awayxGF = sum(away_xGF),
            homeGF = sum(home_GF), awayGF = sum(away_GF), homeCF = sum(home_CF), awayCF = sum(away_CF),
            Home_Up_1 = max(home_lead_1), Home_Up_2 = max(home_lead_2), Home_Up_3 = max(home_lead_3),
            Away_Up_1 = max(away_lead_1), Away_Up_2 = max(away_lead_2), Away_Up_3 = max(away_lead_3), Tied = max(tied), 
            State_5v5 = max(Five), State_4v4 = max(Four), State_3v3 = max(Three)) %>%
  filter(shift_length > 0)

home_as_off <- grouped_shifts %>%
  rename(offense_1 = home_on_1, offense_2 = home_on_2, offense_3 = home_on_3, offense_4 = home_on_4, offense_5 = home_on_5, offense_6 = home_on_6,
         defense_1 = away_on_1, defense_2 = away_on_2, defense_3 = away_on_3, defense_4 = away_on_4, defense_5 = away_on_5, defense_6 = away_on_6,
         Up_1 = Home_Up_1, Up_2 = Home_Up_2, Up_3 = Home_Up_3, Down_1 = Away_Up_1, Down_2 = Away_Up_2, Down_3 = Away_Up_3, xGF = homexGF, GF = homeGF, CF = homeCF) %>%
  select(game_id, shift_change_index, period, game_score_state,
         offense_1, offense_2, offense_3, offense_4, offense_5, offense_6, 
         defense_1, defense_2, defense_3, defense_4, defense_5, defense_6, 
         xGF, GF, CF, shift_length, shift_change_index, Tied, State_5v5, State_4v4, State_3v3, 
         Up_1, Up_2, Up_3, Down_1, Down_2, Down_3) %>%
  mutate(xGF_60 = xGF*3600/shift_length, GF_60 = GF*3600/shift_length, CF_60 = CF*3600/shift_length, is_home = 1)

away_as_off <- grouped_shifts %>%
  rename(offense_1 = away_on_1, offense_2 = away_on_2, offense_3 = away_on_3, offense_4 = away_on_4, offense_5 = away_on_5, offense_6 = away_on_6,
         defense_1 = home_on_1, defense_2 = home_on_2, defense_3 = home_on_3, defense_4 = home_on_4, defense_5 = home_on_5, defense_6 = home_on_6,
         Up_1 = Away_Up_1, Up_2 = Away_Up_2, Up_3 = Away_Up_3, Down_1 = Home_Up_1, Down_2 = Home_Up_2, Down_3 = Home_Up_3, xGF = awayxGF, GF = awayGF, CF = awayCF) %>%
  select(game_id, shift_change_index, period, game_score_state,
         offense_1, offense_2, offense_3, offense_4, offense_5, offense_6, 
         defense_1, defense_2, defense_3, defense_4, defense_5, defense_6, 
         xGF, GF, CF, shift_length, shift_change_index, Tied, State_5v5, State_4v4, State_3v3, 
         Up_1, Up_2, Up_3, Down_1, Down_2, Down_3) %>%
  mutate(xGF_60 = xGF*3600/shift_length, GF_60 = GF*3600/shift_length, CF_60 = CF*3600/shift_length, is_home = 0)

shifts_combined <- full_join(home_as_off, away_as_off)
rm(pbp_ev,home_as_off,away_as_off,grouped_shifts)

shifts_subset = subset(shifts_combined, select = c(offense_1:offense_6,defense_1:defense_6,shift_length,State_4v4,State_3v3,Up_1:Down_3,xGF_60,GF_60,CF_60,is_home))

shifts_combined_dummies_off <- dummy_cols(shifts_subset, select_columns = c("offense_1","offense_2","offense_3","offense_4","offense_5","offense_6"))
shifts_combined_dummies_def <- dummy_cols(shifts_subset, select_columns = c("defense_1","defense_2","defense_3","defense_4","defense_5","defense_6"))
shifts_combined_dummies_off = subset(shifts_combined_dummies_off, select = -c(offense_1,offense_2,offense_3,offense_4,offense_5,offense_6,defense_1,defense_2,defense_3,defense_4,defense_5,defense_6))
shifts_combined_dummies_def = subset(shifts_combined_dummies_def, select = -c(offense_1,offense_2,offense_3,offense_4,offense_5,offense_6,defense_1,defense_2,defense_3,defense_4,defense_5,defense_6,shift_length,State_4v4,State_3v3,Up_1:Down_3,xGF_60,GF_60,CF_60,is_home))
shifts_combined_dummies <- cbind(shifts_combined_dummies_off, shifts_combined_dummies_def)
rm(shifts_subset,shifts_combined_dummies_off,shifts_combined_dummies_def,shifts_combined)

colnames(shifts_combined_dummies) = gsub("offense_1", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("offense_2", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("offense_3", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("offense_4", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("offense_5", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("offense_6", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_1", "defense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_2", "defense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_3", "defense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_4", "defense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_5", "defense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_6", "defense_", colnames(shifts_combined_dummies))

shifts_combined_dummies <- as.data.frame(lapply(split.default(shifts_combined_dummies, names(shifts_combined_dummies)), function(x) Reduce(`+`, x)))

shifts_combined_dummies <- shifts_combined_dummies %>% select(-contains("Goalie"))
shifts_combined_dummies <- shifts_combined_dummies %>% select(-contains("Missing"))

xGF60 <- as.numeric(c(shifts_combined_dummies$xGF_60))
GF60 <- as.numeric(c(shifts_combined_dummies$GF_60))
CF60 <- as.numeric(c(shifts_combined_dummies$CF_60))
shift_length <- as.numeric(c(shifts_combined_dummies$shift_length))
subsetted_dummies = subset(shifts_combined_dummies, select = -c(shift_length, xGF_60, GF_60, CF_60))

RAPM_xGF <- as.matrix(subsetted_dummies)
Sparse_RAPM_xGF <- Matrix(RAPM_xGF, sparse = TRUE)
rm(RAPM_xGF)

RAPM_GF <- as.matrix(subsetted_dummies)
Sparse_RAPM_GF <- Matrix(RAPM_GF, sparse = TRUE)
rm(RAPM_GF)

RAPM_CF <- as.matrix(subsetted_dummies)
Sparse_RAPM_CF <- Matrix(RAPM_CF, sparse = TRUE)
rm(RAPM_CF)

rm(shifts_combined_dummies,subsetted_dummies)
Cross_Validated_Results_xGF <- cv.glmnet(x=Sparse_RAPM_xGF, y=xGF60, weights=shift_length, alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)
Cross_Validated_Results_GF <- cv.glmnet(x=Sparse_RAPM_GF, y=GF60, weights=shift_length, alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)
Cross_Validated_Results_CF <- cv.glmnet(x=Sparse_RAPM_CF, y=CF60, weights=shift_length, alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)

Run_RAPM_xGF <- glmnet(x=Sparse_RAPM_xGF, y=xGF60, weights=shift_length, lambda = Cross_Validated_Results_xGF[["lambda.min"]], alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)
Run_RAPM_GF <- glmnet(x=Sparse_RAPM_GF, y=GF60, weights=shift_length, lambda = Cross_Validated_Results_xGF[["lambda.min"]], alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)
Run_RAPM_CF <- glmnet(x=Sparse_RAPM_CF, y=CF60, weights=shift_length, lambda = Cross_Validated_Results_xGF[["lambda.min"]], alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)

RAPM_xGF_coefficients <- as.data.frame(as.matrix(coef(Run_RAPM_xGF)))
RAPM_GF_coefficients <- as.data.frame(as.matrix(coef(Run_RAPM_GF)))
RAPM_CF_coefficients <- as.data.frame(as.matrix(coef(Run_RAPM_CF)))

Binded_Coefficients_xGF <- cbind(rownames(RAPM_xGF_coefficients), RAPM_xGF_coefficients) %>%
  rename(Player = `rownames(RAPM_xGF_coefficients)`, xGF_60 = s0)
Binded_Coefficients_GF <- cbind(rownames(RAPM_GF_coefficients), RAPM_GF_coefficients) %>%
  rename(Player = `rownames(RAPM_GF_coefficients)`, GF_60 = s0)
Binded_Coefficients_CF <- cbind(rownames(RAPM_CF_coefficients), RAPM_CF_coefficients) %>%
  rename(Player = `rownames(RAPM_CF_coefficients)`, CF_60 = s0)

offense_RAPM_xGF <- Binded_Coefficients_xGF %>%
  filter(grepl("offense", Binded_Coefficients_xGF$Player))
offense_RAPM_GF <- Binded_Coefficients_GF %>%
  filter(grepl("offense", Binded_Coefficients_GF$Player))
offense_RAPM_CF <- Binded_Coefficients_CF %>%
  filter(grepl("offense", Binded_Coefficients_CF$Player))

offense_RAPM_xGF$Player = str_replace_all(offense_RAPM_xGF$Player, "offense__", "")
offense_RAPM_GF$Player = str_replace_all(offense_RAPM_GF$Player, "offense__", "")
offense_RAPM_CF$Player = str_replace_all(offense_RAPM_CF$Player, "offense__", "")

defense_RAPM_xGA <- Binded_Coefficients_xGF %>%
  filter(grepl("defense", Binded_Coefficients_xGF$Player)) %>%
  rename(xGA_60 = xGF_60)
defense_RAPM_GA <- Binded_Coefficients_GF %>%
  filter(grepl("defense", Binded_Coefficients_GF$Player)) %>%
  rename(GA_60 = GF_60)
defense_RAPM_CA <- Binded_Coefficients_CF %>%
  filter(grepl("defense", Binded_Coefficients_CF$Player)) %>%
  rename(CA_60 = CF_60)

defense_RAPM_xGA$Player = str_replace_all(defense_RAPM_xGA$Player, "defense__", "")
defense_RAPM_GA$Player = str_replace_all(defense_RAPM_GA$Player, "defense__", "")
defense_RAPM_CA$Player = str_replace_all(defense_RAPM_CA$Player, "defense__", "")

joined_RAPM_xG <- inner_join(offense_RAPM_xGF, defense_RAPM_xGA, by="Player")
joined_RAPM_G <- inner_join(offense_RAPM_GF, defense_RAPM_GA, by="Player")
joined_RAPM_C <- inner_join(offense_RAPM_CF, defense_RAPM_CA, by="Player")

joined_RAPM_xG$xGPM_60 <- joined_RAPM_xG$xGF_60 - joined_RAPM_xG$xGA_60
joined_RAPM_G$GPM_60 <- joined_RAPM_G$GF_60 - joined_RAPM_G$GA_60
joined_RAPM_C$CPM_60 <- joined_RAPM_C$CF_60 - joined_RAPM_C$CA_60

joined_RAPM <- inner_join(joined_RAPM_xG, joined_RAPM_G, by="Player")
joined_RAPM <- inner_join(joined_RAPM, joined_RAPM_C, by="Player")

joined_RAPM <- joined_RAPM %>%
  arrange(desc(xGPM_60))

joined_RAPM$Player = as.numeric(as.character(joined_RAPM$Player))

joined_RAPM <- joined_RAPM %>% rename(playerID = Player)
rm(defense_RAPM_xGA,defense_RAPM_GA,defense_RAPM_CA,joined_RAPM_G,joined_RAPM_C,joined_RAPM_xG,offense_RAPM_xGF,offense_RAPM_GF,offense_RAPM_CF,Binded_Coefficients_GF,Binded_Coefficients_CF,Binded_Coefficients_xGF,Cross_Validated_Results_GF,Cross_Validated_Results_CF,Cross_Validated_Results_xGF,RAPM_GF_coefficients,RAPM_CF_coefficients,RAPM_xGF_coefficients,RAPM_xGF,Run_RAPM_xGF,Run_RAPM_GF,Run_RAPM_CF,Sparse_RAPM_xGF)
rm(even_strength,GF60,power_play,shift_length,xGF60,Sparse_RAPM_GF,Sparse_RAPM_CF,RAPM_GF,RAPM_CF,CF60)

RAPM <- joined_RAPM

RAPM_even <- RAPM
RAPM_even <- RAPM_even %>%
  rename(RAPM_xGF = xGF_60,RAPM_GF = GF_60,RAPM_CF = CF_60,RAPM_xGA = xGA_60,RAPM_GA = GA_60,RAPM_CA = CA_60)

individual_even <- skater_individual(pbp,"even") %>%
  left_join(toi_5v5) %>%
  mutate(g_60 = (goals/toi)*60,prim_a_60 = (assists_prim/toi)*60,sec_a_60 = (assists_sec/toi)*60,ixg_60 = (ixg/toi)*60,icf_60 = (icf/toi)*60)
individual_even <- na.omit(individual_even)

onice_even <- skater_onice(pbp,"even") %>%
  left_join(toi_5v5) %>%
  mutate(gf_perc = gf/(gf+ga),xgf_perc = xgf/(xgf+xga),cf_perc = cf/(cf+ca),gf_60 = (gf/toi)*60,ga_60 = (ga/toi)*60,xgf_60 = (xgf/toi)*60,xga_60 = (xga/toi)*60,cf_60 = (cf/toi)*60,ca_60 = (ca/toi)*60) %>%
  select(-toi)
onice_even <- na.omit(onice_even)

even_summary <- RAPM_even %>% left_join(individual_even,by="playerID") %>% left_join(onice_even,by="playerID")
even_summary <- even_summary %>%
  filter(toi > toi_threshold_even) %>%
  mutate(EVO = ((((0.75*goals) + (0.7*assists_prim) + (0.55*assists_sec) + (0.075*isog) + (0.1*ixg) + (0.05*cf) + (0.1*xgf) + (0.15*gf)) / gp) + ((RAPM_xGF + (RAPM_GF/5))/2))/2,
         EVD = (RAPM_xGA + (RAPM_GA/5))/2)
even_summary <- even_summary[!is.na(even_summary$EVO),]
even_summary <- even_summary[!is.na(even_summary$EVD),]
even_summary <- even_summary %>%
  select(playerID,EVO,EVD) %>% arrange(desc(EVO))
rm(individual_even,onice_even,toi_5v5,RAPM_even)

pbp <- plays

even_strength <- c("3v3", "4v4", "5v5")
power_play <- c("5v4","5v3","4v3","6v4","6v3")
penalty_kill <- c("4v5","3v5","3v4","4v6","3v6")

pbp <- pbp %>%
  filter(period < 5)

pbp$home_on_1 <- ifelse(is.na(pbp$home_on_1), "MISSING", pbp$home_on_1)
pbp$home_on_2 <- ifelse(is.na(pbp$home_on_2), "MISSING", pbp$home_on_2)
pbp$home_on_3 <- ifelse(is.na(pbp$home_on_3), "MISSING", pbp$home_on_3)
pbp$home_on_4 <- ifelse(is.na(pbp$home_on_4), "MISSING", pbp$home_on_4)
pbp$home_on_5 <- ifelse(is.na(pbp$home_on_5), "MISSING", pbp$home_on_5)
pbp$home_on_6 <- ifelse(is.na(pbp$home_on_6), "MISSING", pbp$home_on_6)
pbp$away_on_1 <- ifelse(is.na(pbp$away_on_1), "MISSING", pbp$away_on_1)
pbp$away_on_2 <- ifelse(is.na(pbp$away_on_2), "MISSING", pbp$away_on_2)
pbp$away_on_3 <- ifelse(is.na(pbp$away_on_3), "MISSING", pbp$away_on_3)
pbp$away_on_4 <- ifelse(is.na(pbp$away_on_4), "MISSING", pbp$away_on_4)
pbp$away_on_5 <- ifelse(is.na(pbp$away_on_5), "MISSING", pbp$away_on_5)
pbp$away_on_6 <- ifelse(is.na(pbp$away_on_6), "MISSING", pbp$away_on_6)

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
pbp$home_xGF <- ifelse(pbp$event_team_name==pbp$home_team, pbp$xg, 0)
pbp$away_xGF <- ifelse(pbp$event_team_name!=pbp$home_team, pbp$xg, 0)
pbp$home_GF <- ifelse(pbp$event_type=="GOAL" & pbp$event_team_name==pbp$home_team, 1, 0)
pbp$away_GF <- ifelse(pbp$event_type=="GOAL" & pbp$event_team_name==pbp$away_team, 1, 0)
pbp$home_CF <- ifelse(pbp$event_type %in% c("GOAL","SHOT_ON_GOAL","MISSED_SHOT","BLOCKED_SHOT") & pbp$event_team_name==pbp$home_team, 1, 0)
pbp$away_CF <- ifelse(pbp$event_type %in% c("GOAL","SHOT_ON_GOAL","MISSED_SHOT","BLOCKED_SHOT") & pbp$event_team_name==pbp$away_team, 1, 0)
pbp$tied <- ifelse(pbp$home_score==pbp$away_score, 1, 0)
pbp$home_lead_1 <- ifelse(pbp$home_score-pbp$away_score==1, 1, 0)
pbp$home_lead_2 <- ifelse(pbp$home_score-pbp$away_score==2, 1, 0)
pbp$home_lead_3 <- ifelse(pbp$home_score-pbp$away_score>=3, 1, 0)
pbp$away_lead_1 <- ifelse(pbp$home_score-pbp$away_score==(-1), 1, 0)
pbp$away_lead_2 <- ifelse(pbp$home_score-pbp$away_score==(-2), 1, 0)
pbp$away_lead_3 <- ifelse(pbp$home_score-pbp$away_score<=(-3), 1, 0)
pbp$Five <- ifelse(pbp$strength_state=="5v5", 1, 0)
pbp$Four <- ifelse(pbp$strength_state=="4v4", 1, 0)
pbp$Three <- ifelse(pbp$strength_state=="3v3", 1, 0)
pbp$home_xGF[is.na(pbp$home_xGF)] <- 0
pbp$away_xGF[is.na(pbp$away_xGF)] <- 0
pbp$event_length[is.na(pbp$event_length)] <- 0

pbp_ev <- pbp %>%
  filter(strength_state %in% power_play)

grouped_shifts <- pbp_ev %>%
  group_by(game_id, shift_change_index, period, game_score_state, home_on_1, home_on_2, home_on_3, home_on_4, home_on_5, home_on_6, 
           away_on_1, away_on_2, away_on_3, away_on_4, away_on_5, away_on_6) %>%
  summarise(shift_length = sum(event_length), homexGF = sum(home_xGF), awayxGF = sum(away_xGF),
            homeGF = sum(home_GF), awayGF = sum(away_GF), homeCF = sum(home_CF), awayCF = sum(away_CF),
            Home_Up_1 = max(home_lead_1), Home_Up_2 = max(home_lead_2), Home_Up_3 = max(home_lead_3),
            Away_Up_1 = max(away_lead_1), Away_Up_2 = max(away_lead_2), Away_Up_3 = max(away_lead_3), Tied = max(tied), 
            State_5v5 = max(Five), State_4v4 = max(Four), State_3v3 = max(Three)) %>%
  filter(shift_length > 0)

home_as_off <- grouped_shifts %>%
  rename(offense_1 = home_on_1, offense_2 = home_on_2, offense_3 = home_on_3, offense_4 = home_on_4, offense_5 = home_on_5, offense_6 = home_on_6,
         defense_1 = away_on_1, defense_2 = away_on_2, defense_3 = away_on_3, defense_4 = away_on_4, defense_5 = away_on_5, defense_6 = away_on_6,
         Up_1 = Home_Up_1, Up_2 = Home_Up_2, Up_3 = Home_Up_3, Down_1 = Away_Up_1, Down_2 = Away_Up_2, Down_3 = Away_Up_3, xGF = homexGF, GF = homeGF, CF = homeCF) %>%
  select(game_id, shift_change_index, period, game_score_state,
         offense_1, offense_2, offense_3, offense_4, offense_5, offense_6, 
         defense_1, defense_2, defense_3, defense_4, defense_5, defense_6, 
         xGF, GF, CF, shift_length, shift_change_index, Tied, State_5v5, State_4v4, State_3v3, 
         Up_1, Up_2, Up_3, Down_1, Down_2, Down_3) %>%
  mutate(xGF_60 = xGF*3600/shift_length, GF_60 = GF*3600/shift_length, CF_60 = CF*3600/shift_length, is_home = 1)

away_as_off <- grouped_shifts %>%
  rename(offense_1 = away_on_1, offense_2 = away_on_2, offense_3 = away_on_3, offense_4 = away_on_4, offense_5 = away_on_5, offense_6 = away_on_6,
         defense_1 = home_on_1, defense_2 = home_on_2, defense_3 = home_on_3, defense_4 = home_on_4, defense_5 = home_on_5, defense_6 = home_on_6,
         Up_1 = Away_Up_1, Up_2 = Away_Up_2, Up_3 = Away_Up_3, Down_1 = Home_Up_1, Down_2 = Home_Up_2, Down_3 = Home_Up_3, xGF = awayxGF, GF = awayGF, CF = awayCF) %>%
  select(game_id, shift_change_index, period, game_score_state,
         offense_1, offense_2, offense_3, offense_4, offense_5, offense_6, 
         defense_1, defense_2, defense_3, defense_4, defense_5, defense_6, 
         xGF, GF, CF, shift_length, shift_change_index, Tied, State_5v5, State_4v4, State_3v3, 
         Up_1, Up_2, Up_3, Down_1, Down_2, Down_3) %>%
  mutate(xGF_60 = xGF*3600/shift_length, GF_60 = GF*3600/shift_length, CF_60 = CF*3600/shift_length, is_home = 0)

shifts_combined <- full_join(home_as_off, away_as_off)
rm(pbp_ev,home_as_off,away_as_off,grouped_shifts)

shifts_subset = subset(shifts_combined, select = c(offense_1:offense_6,defense_1:defense_6,shift_length,State_4v4,State_3v3,Up_1:Down_3,xGF_60,GF_60,CF_60,is_home))

shifts_combined_dummies_off <- dummy_cols(shifts_subset, select_columns = c("offense_1","offense_2","offense_3","offense_4","offense_5","offense_6"))
shifts_combined_dummies_def <- dummy_cols(shifts_subset, select_columns = c("defense_1","defense_2","defense_3","defense_4","defense_5","defense_6"))
shifts_combined_dummies_off = subset(shifts_combined_dummies_off, select = -c(offense_1,offense_2,offense_3,offense_4,offense_5,offense_6,defense_1,defense_2,defense_3,defense_4,defense_5,defense_6))
shifts_combined_dummies_def = subset(shifts_combined_dummies_def, select = -c(offense_1,offense_2,offense_3,offense_4,offense_5,offense_6,defense_1,defense_2,defense_3,defense_4,defense_5,defense_6,shift_length,State_4v4,State_3v3,Up_1:Down_3,xGF_60,GF_60,CF_60,is_home))
shifts_combined_dummies <- cbind(shifts_combined_dummies_off, shifts_combined_dummies_def)
rm(shifts_subset,shifts_combined_dummies_off,shifts_combined_dummies_def,shifts_combined)

colnames(shifts_combined_dummies) = gsub("offense_1", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("offense_2", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("offense_3", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("offense_4", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("offense_5", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("offense_6", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_1", "defense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_2", "defense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_3", "defense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_4", "defense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_5", "defense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_6", "defense_", colnames(shifts_combined_dummies))

shifts_combined_dummies <- as.data.frame(lapply(split.default(shifts_combined_dummies, names(shifts_combined_dummies)), function(x) Reduce(`+`, x)))

shifts_combined_dummies <- shifts_combined_dummies %>% select(-contains("Goalie"))
shifts_combined_dummies <- shifts_combined_dummies %>% select(-contains("Missing"))

xGF60 <- as.numeric(c(shifts_combined_dummies$xGF_60))
GF60 <- as.numeric(c(shifts_combined_dummies$GF_60))
CF60 <- as.numeric(c(shifts_combined_dummies$CF_60))
shift_length <- as.numeric(c(shifts_combined_dummies$shift_length))
subsetted_dummies = subset(shifts_combined_dummies, select = -c(shift_length, xGF_60, GF_60, CF_60))

RAPM_xGF <- as.matrix(subsetted_dummies)
Sparse_RAPM_xGF <- Matrix(RAPM_xGF, sparse = TRUE)
rm(RAPM_xGF)

RAPM_GF <- as.matrix(subsetted_dummies)
Sparse_RAPM_GF <- Matrix(RAPM_GF, sparse = TRUE)
rm(RAPM_GF)

RAPM_CF <- as.matrix(subsetted_dummies)
Sparse_RAPM_CF <- Matrix(RAPM_CF, sparse = TRUE)
rm(RAPM_CF)

rm(shifts_combined_dummies,subsetted_dummies)
Cross_Validated_Results_xGF <- cv.glmnet(x=Sparse_RAPM_xGF, y=xGF60, weights=shift_length, alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)
Cross_Validated_Results_GF <- cv.glmnet(x=Sparse_RAPM_GF, y=GF60, weights=shift_length, alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)
Cross_Validated_Results_CF <- cv.glmnet(x=Sparse_RAPM_CF, y=CF60, weights=shift_length, alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)

Run_RAPM_xGF <- glmnet(x=Sparse_RAPM_xGF, y=xGF60, weights=shift_length, lambda = Cross_Validated_Results_xGF[["lambda.min"]], alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)
Run_RAPM_GF <- glmnet(x=Sparse_RAPM_GF, y=GF60, weights=shift_length, lambda = Cross_Validated_Results_xGF[["lambda.min"]], alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)
Run_RAPM_CF <- glmnet(x=Sparse_RAPM_CF, y=CF60, weights=shift_length, lambda = Cross_Validated_Results_xGF[["lambda.min"]], alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)

RAPM_xGF_coefficients <- as.data.frame(as.matrix(coef(Run_RAPM_xGF)))
RAPM_GF_coefficients <- as.data.frame(as.matrix(coef(Run_RAPM_GF)))
RAPM_CF_coefficients <- as.data.frame(as.matrix(coef(Run_RAPM_CF)))


Binded_Coefficients_xGF <- cbind(rownames(RAPM_xGF_coefficients), RAPM_xGF_coefficients) %>%
  rename(Player = `rownames(RAPM_xGF_coefficients)`, xGF_60 = s0)
Binded_Coefficients_GF <- cbind(rownames(RAPM_GF_coefficients), RAPM_GF_coefficients) %>%
  rename(Player = `rownames(RAPM_GF_coefficients)`, GF_60 = s0)
Binded_Coefficients_CF <- cbind(rownames(RAPM_CF_coefficients), RAPM_CF_coefficients) %>%
  rename(Player = `rownames(RAPM_CF_coefficients)`, CF_60 = s0)

offense_RAPM_xGF <- Binded_Coefficients_xGF %>%
  filter(grepl("offense", Binded_Coefficients_xGF$Player))
offense_RAPM_GF <- Binded_Coefficients_GF %>%
  filter(grepl("offense", Binded_Coefficients_GF$Player))
offense_RAPM_CF <- Binded_Coefficients_CF %>%
  filter(grepl("offense", Binded_Coefficients_CF$Player))

offense_RAPM_xGF$Player = str_replace_all(offense_RAPM_xGF$Player, "offense__", "")
offense_RAPM_GF$Player = str_replace_all(offense_RAPM_GF$Player, "offense__", "")
offense_RAPM_CF$Player = str_replace_all(offense_RAPM_CF$Player, "offense__", "")

defense_RAPM_xGA <- Binded_Coefficients_xGF %>%
  filter(grepl("defense", Binded_Coefficients_xGF$Player)) %>%
  rename(xGA_60 = xGF_60)
defense_RAPM_GA <- Binded_Coefficients_GF %>%
  filter(grepl("defense", Binded_Coefficients_GF$Player)) %>%
  rename(GA_60 = GF_60)
defense_RAPM_CA <- Binded_Coefficients_CF %>%
  filter(grepl("defense", Binded_Coefficients_CF$Player)) %>%
  rename(CA_60 = CF_60)

defense_RAPM_xGA$Player = str_replace_all(defense_RAPM_xGA$Player, "defense__", "")
defense_RAPM_GA$Player = str_replace_all(defense_RAPM_GA$Player, "defense__", "")
defense_RAPM_CA$Player = str_replace_all(defense_RAPM_CA$Player, "defense__", "")

joined_RAPM_xG <- inner_join(offense_RAPM_xGF, defense_RAPM_xGA, by="Player")
joined_RAPM_G <- inner_join(offense_RAPM_GF, defense_RAPM_GA, by="Player")
joined_RAPM_C <- inner_join(offense_RAPM_CF, defense_RAPM_CA, by="Player")

joined_RAPM_xG$xGPM_60 <- joined_RAPM_xG$xGF_60 - joined_RAPM_xG$xGA_60
joined_RAPM_G$GPM_60 <- joined_RAPM_G$GF_60 - joined_RAPM_G$GA_60
joined_RAPM_C$CPM_60 <- joined_RAPM_C$CF_60 - joined_RAPM_C$CA_60

joined_RAPM <- inner_join(joined_RAPM_xG, joined_RAPM_G, by="Player")
joined_RAPM <- inner_join(joined_RAPM, joined_RAPM_C, by="Player")

joined_RAPM <- joined_RAPM %>%
  arrange(desc(xGPM_60))

joined_RAPM$Player = as.numeric(as.character(joined_RAPM$Player))

joined_RAPM <- joined_RAPM %>% rename(playerID = Player)
rm(defense_RAPM_xGA,defense_RAPM_GA,defense_RAPM_CA,joined_RAPM_G,joined_RAPM_C,joined_RAPM_xG,offense_RAPM_xGF,offense_RAPM_GF,offense_RAPM_CF,Binded_Coefficients_GF,Binded_Coefficients_CF,Binded_Coefficients_xGF,Cross_Validated_Results_GF,Cross_Validated_Results_CF,Cross_Validated_Results_xGF,RAPM_GF_coefficients,RAPM_CF_coefficients,RAPM_xGF_coefficients,RAPM_xGF,Run_RAPM_xGF,Run_RAPM_GF,Run_RAPM_CF,Sparse_RAPM_xGF)
rm(even_strength,GF60,power_play,shift_length,xGF60,Sparse_RAPM_GF,Sparse_RAPM_CF,RAPM_GF,RAPM_CF,CF60)

RAPM <- joined_RAPM

RAPM_PP <- RAPM
RAPM_PP <- RAPM_PP %>%
  rename(RAPM_xGF = xGF_60,RAPM_GF = GF_60,RAPM_CF = CF_60,RAPM_xGA = xGA_60,RAPM_GA = GA_60,RAPM_CA = CA_60)

individual_pp <- skater_individual(pbp,"pp") %>%
  left_join(toi_pp) %>%
  mutate(g_60 = (goals/toi)*60,prim_a_60 = (assists_prim/toi)*60, sec_a_60 = (assists_sec/toi)*60,isog_60 = (isog/toi)*60,ixg_60 = (ixg/toi)*60,icf_60 = (icf/toi)*60)
individual_pp <- na.omit(individual_pp)

onice_pp <- skater_onice(pbp,"pp") %>%
  left_join(toi_pp) %>%
  mutate(gf_perc = gf/(gf+ga),xgf_perc = xgf/(xgf+xga),cf_perc = cf/(cf+ca),gf_60 = (gf/toi)*60,ga_60 = (ga/toi)*60,xgf_60 = (xgf/toi)*60,xga_60 = (xga/toi)*60,cf_60 = (cf/toi)*60,ca_60 = (ca/toi)*60) %>%
  select(-toi)
onice_pp <- na.omit(onice_pp)

pp_summary <- RAPM_PP %>% left_join(individual_pp,by="playerID") %>% left_join(onice_pp,by="playerID")
pp_summary <- pp_summary %>% filter(toi>toi_threshold_spec)
pp_summary <- pp_summary %>%
  mutate(PPO = ((((0.75*g_60) + (0.7*prim_a_60) + (0.55*sec_a_60) + (0.075*isog_60) + (0.1*ixg_60) + (0.05*cf_60) + (0.1*xgf_60) + (0.15*gf_60)) / gp) + ((RAPM_xGF + (RAPM_GF/5))/2))/2)
pp_summary <- pp_summary[!is.na(pp_summary$PPO),]
pp_summary <- pp_summary %>%
  select(playerID,PPO) %>% arrange(desc(PPO))
rm(individual_pp,onice_pp,RAPM_PP,toi_pp)

pbp <- plays

even_strength <- c("3v3", "4v4", "5v5")
power_play <- c("5v4","5v3","4v3","6v4","6v3")
penalty_kill <- c("4v5","3v5","3v4","4v6","3v6")

pbp <- pbp %>%
  filter(period < 5)

pbp$home_on_1 <- ifelse(is.na(pbp$home_on_1), "MISSING", pbp$home_on_1)
pbp$home_on_2 <- ifelse(is.na(pbp$home_on_2), "MISSING", pbp$home_on_2)
pbp$home_on_3 <- ifelse(is.na(pbp$home_on_3), "MISSING", pbp$home_on_3)
pbp$home_on_4 <- ifelse(is.na(pbp$home_on_4), "MISSING", pbp$home_on_4)
pbp$home_on_5 <- ifelse(is.na(pbp$home_on_5), "MISSING", pbp$home_on_5)
pbp$home_on_6 <- ifelse(is.na(pbp$home_on_6), "MISSING", pbp$home_on_6)
pbp$away_on_1 <- ifelse(is.na(pbp$away_on_1), "MISSING", pbp$away_on_1)
pbp$away_on_2 <- ifelse(is.na(pbp$away_on_2), "MISSING", pbp$away_on_2)
pbp$away_on_3 <- ifelse(is.na(pbp$away_on_3), "MISSING", pbp$away_on_3)
pbp$away_on_4 <- ifelse(is.na(pbp$away_on_4), "MISSING", pbp$away_on_4)
pbp$away_on_5 <- ifelse(is.na(pbp$away_on_5), "MISSING", pbp$away_on_5)
pbp$away_on_6 <- ifelse(is.na(pbp$away_on_6), "MISSING", pbp$away_on_6)

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
pbp$home_xGF <- ifelse(pbp$event_team_name==pbp$home_team, pbp$xg, 0)
pbp$away_xGF <- ifelse(pbp$event_team_name!=pbp$home_team, pbp$xg, 0)
pbp$home_GF <- ifelse(pbp$event_type=="GOAL" & pbp$event_team_name==pbp$home_team, 1, 0)
pbp$away_GF <- ifelse(pbp$event_type=="GOAL" & pbp$event_team_name==pbp$away_team, 1, 0)
pbp$home_CF <- ifelse(pbp$event_type %in% c("GOAL","SHOT_ON_GOAL","MISSED_SHOT","BLOCKED_SHOT") & pbp$event_team_name==pbp$home_team, 1, 0)
pbp$away_CF <- ifelse(pbp$event_type %in% c("GOAL","SHOT_ON_GOAL","MISSED_SHOT","BLOCKED_SHOT") & pbp$event_team_name==pbp$away_team, 1, 0)
pbp$tied <- ifelse(pbp$home_score==pbp$away_score, 1, 0)
pbp$home_lead_1 <- ifelse(pbp$home_score-pbp$away_score==1, 1, 0)
pbp$home_lead_2 <- ifelse(pbp$home_score-pbp$away_score==2, 1, 0)
pbp$home_lead_3 <- ifelse(pbp$home_score-pbp$away_score>=3, 1, 0)
pbp$away_lead_1 <- ifelse(pbp$home_score-pbp$away_score==(-1), 1, 0)
pbp$away_lead_2 <- ifelse(pbp$home_score-pbp$away_score==(-2), 1, 0)
pbp$away_lead_3 <- ifelse(pbp$home_score-pbp$away_score<=(-3), 1, 0)
pbp$Five <- ifelse(pbp$strength_state=="5v5", 1, 0)
pbp$Four <- ifelse(pbp$strength_state=="4v4", 1, 0)
pbp$Three <- ifelse(pbp$strength_state=="3v3", 1, 0)
pbp$home_xGF[is.na(pbp$home_xGF)] <- 0
pbp$away_xGF[is.na(pbp$away_xGF)] <- 0
pbp$event_length[is.na(pbp$event_length)] <- 0

pbp_ev <- pbp %>%
  filter(strength_state %in% penalty_kill)

grouped_shifts <- pbp_ev %>%
  group_by(game_id, shift_change_index, period, game_score_state, home_on_1, home_on_2, home_on_3, home_on_4, home_on_5, home_on_6, 
           away_on_1, away_on_2, away_on_3, away_on_4, away_on_5, away_on_6) %>%
  summarise(shift_length = sum(event_length), homexGF = sum(home_xGF), awayxGF = sum(away_xGF),
            homeGF = sum(home_GF), awayGF = sum(away_GF), homeCF = sum(home_CF), awayCF = sum(away_CF),
            Home_Up_1 = max(home_lead_1), Home_Up_2 = max(home_lead_2), Home_Up_3 = max(home_lead_3),
            Away_Up_1 = max(away_lead_1), Away_Up_2 = max(away_lead_2), Away_Up_3 = max(away_lead_3), Tied = max(tied), 
            State_5v5 = max(Five), State_4v4 = max(Four), State_3v3 = max(Three)) %>%
  filter(shift_length > 0)

home_as_off <- grouped_shifts %>%
  rename(offense_1 = home_on_1, offense_2 = home_on_2, offense_3 = home_on_3, offense_4 = home_on_4, offense_5 = home_on_5, offense_6 = home_on_6,
         defense_1 = away_on_1, defense_2 = away_on_2, defense_3 = away_on_3, defense_4 = away_on_4, defense_5 = away_on_5, defense_6 = away_on_6,
         Up_1 = Home_Up_1, Up_2 = Home_Up_2, Up_3 = Home_Up_3, Down_1 = Away_Up_1, Down_2 = Away_Up_2, Down_3 = Away_Up_3, xGF = homexGF, GF = homeGF, CF = homeCF) %>%
  select(game_id, shift_change_index, period, game_score_state,
         offense_1, offense_2, offense_3, offense_4, offense_5, offense_6, 
         defense_1, defense_2, defense_3, defense_4, defense_5, defense_6, 
         xGF, GF, CF, shift_length, shift_change_index, Tied, State_5v5, State_4v4, State_3v3, 
         Up_1, Up_2, Up_3, Down_1, Down_2, Down_3) %>%
  mutate(xGF_60 = xGF*3600/shift_length, GF_60 = GF*3600/shift_length, CF_60 = CF*3600/shift_length, is_home = 1)

away_as_off <- grouped_shifts %>%
  rename(offense_1 = away_on_1, offense_2 = away_on_2, offense_3 = away_on_3, offense_4 = away_on_4, offense_5 = away_on_5, offense_6 = away_on_6,
         defense_1 = home_on_1, defense_2 = home_on_2, defense_3 = home_on_3, defense_4 = home_on_4, defense_5 = home_on_5, defense_6 = home_on_6,
         Up_1 = Away_Up_1, Up_2 = Away_Up_2, Up_3 = Away_Up_3, Down_1 = Home_Up_1, Down_2 = Home_Up_2, Down_3 = Home_Up_3, xGF = awayxGF, GF = awayGF, CF = awayCF) %>%
  select(game_id, shift_change_index, period, game_score_state,
         offense_1, offense_2, offense_3, offense_4, offense_5, offense_6, 
         defense_1, defense_2, defense_3, defense_4, defense_5, defense_6, 
         xGF, GF, CF, shift_length, shift_change_index, Tied, State_5v5, State_4v4, State_3v3, 
         Up_1, Up_2, Up_3, Down_1, Down_2, Down_3) %>%
  mutate(xGF_60 = xGF*3600/shift_length, GF_60 = GF*3600/shift_length, CF_60 = CF*3600/shift_length, is_home = 0)

shifts_combined <- full_join(home_as_off, away_as_off)
rm(pbp_ev,home_as_off,away_as_off,grouped_shifts)

shifts_subset = subset(shifts_combined, select = c(offense_1:offense_6,defense_1:defense_6,shift_length,State_4v4,State_3v3,Up_1:Down_3,xGF_60,GF_60,CF_60,is_home))

shifts_combined_dummies_off <- dummy_cols(shifts_subset, select_columns = c("offense_1","offense_2","offense_3","offense_4","offense_5","offense_6"))
shifts_combined_dummies_def <- dummy_cols(shifts_subset, select_columns = c("defense_1","defense_2","defense_3","defense_4","defense_5","defense_6"))
shifts_combined_dummies_off = subset(shifts_combined_dummies_off, select = -c(offense_1,offense_2,offense_3,offense_4,offense_5,offense_6,defense_1,defense_2,defense_3,defense_4,defense_5,defense_6))
shifts_combined_dummies_def = subset(shifts_combined_dummies_def, select = -c(offense_1,offense_2,offense_3,offense_4,offense_5,offense_6,defense_1,defense_2,defense_3,defense_4,defense_5,defense_6,shift_length,State_4v4,State_3v3,Up_1:Down_3,xGF_60,GF_60,CF_60,is_home))
shifts_combined_dummies <- cbind(shifts_combined_dummies_off, shifts_combined_dummies_def)
rm(shifts_subset,shifts_combined_dummies_off,shifts_combined_dummies_def,shifts_combined)

colnames(shifts_combined_dummies) = gsub("offense_1", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("offense_2", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("offense_3", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("offense_4", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("offense_5", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("offense_6", "offense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_1", "defense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_2", "defense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_3", "defense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_4", "defense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_5", "defense_", colnames(shifts_combined_dummies))
colnames(shifts_combined_dummies) = gsub("defense_6", "defense_", colnames(shifts_combined_dummies))

shifts_combined_dummies <- as.data.frame(lapply(split.default(shifts_combined_dummies, names(shifts_combined_dummies)), function(x) Reduce(`+`, x)))

shifts_combined_dummies <- shifts_combined_dummies %>% select(-contains("Goalie"))
shifts_combined_dummies <- shifts_combined_dummies %>% select(-contains("Missing"))

xGF60 <- as.numeric(c(shifts_combined_dummies$xGF_60))
GF60 <- as.numeric(c(shifts_combined_dummies$GF_60))
CF60 <- as.numeric(c(shifts_combined_dummies$CF_60))
shift_length <- as.numeric(c(shifts_combined_dummies$shift_length))
subsetted_dummies = subset(shifts_combined_dummies, select = -c(shift_length, xGF_60, GF_60, CF_60))

RAPM_xGF <- as.matrix(subsetted_dummies)
Sparse_RAPM_xGF <- Matrix(RAPM_xGF, sparse = TRUE)
rm(RAPM_xGF)

RAPM_GF <- as.matrix(subsetted_dummies)
Sparse_RAPM_GF <- Matrix(RAPM_GF, sparse = TRUE)
rm(RAPM_GF)

RAPM_CF <- as.matrix(subsetted_dummies)
Sparse_RAPM_CF <- Matrix(RAPM_CF, sparse = TRUE)
rm(RAPM_CF)

rm(shifts_combined_dummies,subsetted_dummies)
Cross_Validated_Results_xGF <- cv.glmnet(x=Sparse_RAPM_xGF, y=xGF60, weights=shift_length, alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)
Cross_Validated_Results_GF <- cv.glmnet(x=Sparse_RAPM_GF, y=GF60, weights=shift_length, alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)
Cross_Validated_Results_CF <- cv.glmnet(x=Sparse_RAPM_CF, y=CF60, weights=shift_length, alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)

Run_RAPM_xGF <- glmnet(x=Sparse_RAPM_xGF, y=xGF60, weights=shift_length, lambda = Cross_Validated_Results_xGF[["lambda.min"]], alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)
Run_RAPM_GF <- glmnet(x=Sparse_RAPM_GF, y=GF60, weights=shift_length, lambda = Cross_Validated_Results_xGF[["lambda.min"]], alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)
Run_RAPM_CF <- glmnet(x=Sparse_RAPM_CF, y=CF60, weights=shift_length, lambda = Cross_Validated_Results_xGF[["lambda.min"]], alpha=0, nfolds=10, standardize=FALSE, parallel=TRUE)

RAPM_xGF_coefficients <- as.data.frame(as.matrix(coef(Run_RAPM_xGF)))
RAPM_GF_coefficients <- as.data.frame(as.matrix(coef(Run_RAPM_GF)))
RAPM_CF_coefficients <- as.data.frame(as.matrix(coef(Run_RAPM_CF)))

Binded_Coefficients_xGF <- cbind(rownames(RAPM_xGF_coefficients), RAPM_xGF_coefficients) %>%
  rename(Player = `rownames(RAPM_xGF_coefficients)`, xGF_60 = s0)
Binded_Coefficients_GF <- cbind(rownames(RAPM_GF_coefficients), RAPM_GF_coefficients) %>%
  rename(Player = `rownames(RAPM_GF_coefficients)`, GF_60 = s0)
Binded_Coefficients_CF <- cbind(rownames(RAPM_CF_coefficients), RAPM_CF_coefficients) %>%
  rename(Player = `rownames(RAPM_CF_coefficients)`, CF_60 = s0)

offense_RAPM_xGF <- Binded_Coefficients_xGF %>%
  filter(grepl("offense", Binded_Coefficients_xGF$Player))
offense_RAPM_GF <- Binded_Coefficients_GF %>%
  filter(grepl("offense", Binded_Coefficients_GF$Player))
offense_RAPM_CF <- Binded_Coefficients_CF %>%
  filter(grepl("offense", Binded_Coefficients_CF$Player))

offense_RAPM_xGF$Player = str_replace_all(offense_RAPM_xGF$Player, "offense__", "")
offense_RAPM_GF$Player = str_replace_all(offense_RAPM_GF$Player, "offense__", "")
offense_RAPM_CF$Player = str_replace_all(offense_RAPM_CF$Player, "offense__", "")

defense_RAPM_xGA <- Binded_Coefficients_xGF %>%
  filter(grepl("defense", Binded_Coefficients_xGF$Player)) %>%
  rename(xGA_60 = xGF_60)
defense_RAPM_GA <- Binded_Coefficients_GF %>%
  filter(grepl("defense", Binded_Coefficients_GF$Player)) %>%
  rename(GA_60 = GF_60)
defense_RAPM_CA <- Binded_Coefficients_CF %>%
  filter(grepl("defense", Binded_Coefficients_CF$Player)) %>%
  rename(CA_60 = CF_60)

defense_RAPM_xGA$Player = str_replace_all(defense_RAPM_xGA$Player, "defense__", "")
defense_RAPM_GA$Player = str_replace_all(defense_RAPM_GA$Player, "defense__", "")
defense_RAPM_CA$Player = str_replace_all(defense_RAPM_CA$Player, "defense__", "")

joined_RAPM_xG <- inner_join(offense_RAPM_xGF, defense_RAPM_xGA, by="Player")
joined_RAPM_G <- inner_join(offense_RAPM_GF, defense_RAPM_GA, by="Player")
joined_RAPM_C <- inner_join(offense_RAPM_CF, defense_RAPM_CA, by="Player")

joined_RAPM_xG$xGPM_60 <- joined_RAPM_xG$xGF_60 - joined_RAPM_xG$xGA_60
joined_RAPM_G$GPM_60 <- joined_RAPM_G$GF_60 - joined_RAPM_G$GA_60
joined_RAPM_C$CPM_60 <- joined_RAPM_C$CF_60 - joined_RAPM_C$CA_60

joined_RAPM <- inner_join(joined_RAPM_xG, joined_RAPM_G, by="Player")
joined_RAPM <- inner_join(joined_RAPM, joined_RAPM_C, by="Player")

joined_RAPM <- joined_RAPM %>%
  arrange(desc(xGPM_60))

joined_RAPM$Player = as.numeric(as.character(joined_RAPM$Player))

joined_RAPM <- joined_RAPM %>% rename(playerID = Player)
rm(defense_RAPM_xGA,defense_RAPM_GA,defense_RAPM_CA,joined_RAPM_G,joined_RAPM_C,joined_RAPM_xG,offense_RAPM_xGF,offense_RAPM_GF,offense_RAPM_CF,Binded_Coefficients_GF,Binded_Coefficients_CF,Binded_Coefficients_xGF,Cross_Validated_Results_GF,Cross_Validated_Results_CF,Cross_Validated_Results_xGF,RAPM_GF_coefficients,RAPM_CF_coefficients,RAPM_xGF_coefficients,RAPM_xGF,Run_RAPM_xGF,Run_RAPM_GF,Run_RAPM_CF,Sparse_RAPM_xGF)
rm(even_strength,GF60,power_play,shift_length,xGF60,Sparse_RAPM_GF,Sparse_RAPM_CF,RAPM_GF,RAPM_CF,CF60)

RAPM <- joined_RAPM

RAPM_PK <- RAPM
RAPM_PK <- RAPM_PK %>%
  rename(RAPM_xGF = xGF_60,RAPM_GF = GF_60,RAPM_CF = CF_60,RAPM_xGA = xGA_60,RAPM_GA = GA_60,RAPM_CA = CA_60)

individual_pk <- skater_individual(pbp,"pk") %>% select(playerID,gp)
individual_pk <- na.omit(individual_pk)

onice_pk <- skater_onice(pbp,"pp") %>%
  left_join(toi_pk) %>%
  mutate(ga_60 = (ga/toi)*60,xga_60 = (xga/toi)*60,ca_60 = (ca/toi)*60) %>%
  select(playerID,ga,xga,ca,fa,toi,ga_60,xga_60,ca_60)
onice_pk <- na.omit(onice_pk)

pk_summary <- RAPM_PK %>% left_join(individual_pk,by="playerID") %>% left_join(onice_pk,by="playerID")
pk_summary <- pk_summary %>% filter(toi>toi_threshold_spec)
pk_summary <- pk_summary %>%
  mutate(SHD = (RAPM_xGA + (RAPM_GA/5))/2)
pk_summary <- pk_summary[!is.na(pk_summary$SHD),]
pk_summary <- pk_summary %>%
  select(playerID,SHD) %>% arrange(desc(SHD))
rm(individual_pk,onice_pk,RAPM_PK,toi_pk)

pbp <- plays

finishing_summary <- player_summary %>%
  mutate(FIN = (gax/toi)*ixg) %>%
  select(playerID,FIN)

penalty_summary <- player_summary %>%
  mutate(pens_against = (pens_taken/toi),
         pens_for = (pens_drawn/toi),
         PEN = (pens_for - pens_against)) %>%
  select(playerID,PEN)

player_summary <- all_summary %>% 
  left_join(even_summary,by="playerID") %>%
  left_join(pp_summary,by="playerID") %>%
  left_join(pk_summary,by="playerID") %>%
  left_join(finishing_summary,by="playerID") %>%
  left_join(penalty_summary,by="playerID") %>%
  mutate(
    across(PPO:PEN, ~replace_na(.x,0))
  ) %>%
  mutate(EVO = EVO,EVD = (EVD*0.78),PPO = (PPO*0.28),SHD = (SHD*0.2),FIN = (FIN*1.025),PEN = (PEN*15),
         OVR = EVO - EVD + PPO - SHD + FIN + PEN) %>%
  relocate(OVR,EVO,EVD,PPO,SHD,FIN,PEN,game_score,.after = playerID) %>%
  arrange(desc(OVR)) %>%
  filter(toi > toi_threshold_even)
player_summary <- na.omit(player_summary)

playernames <- dbReadTable(con,"player_data")
ids <- playernames$playerID
ids_to_check <- player_summary$playerID
ids_to_add <- setdiff(ids_to_check,ids)
if(length(ids_to_add)>0){
  for(i in 1:length(ids_to_add)){
    newplayer <- player(ids_to_add[i])
    playernames <- rbind(playernames,newplayer)
  }
  dbWriteTable(con, name = "player_data", value = playernames, row.names = FALSE, overwrite = TRUE)
}

player_summary <- playernames %>% left_join(player_summary, by="playerID")
player_summary <- na.omit(player_summary)
dbWriteTable(con, name = "player_stats__for_sim", value = player_summary, row.names = FALSE, overwrite = TRUE)

pbp <- plays
goalie_games <- min(10,(length(unique(pbp$game_id))/16)/5)
goalie_summary <- goalie_stats(pbp,"all")
goalie_summary <- goalie_summary %>%
  filter(gp>goalie_games) %>%
  mutate(GOA = (gsax/shots_against)*xga/4+0.55) %>%
  relocate(GOA,.after = playerID) %>%
  arrange(desc(GOA))

playernames <- dbReadTable(con,"player_data")
ids <- playernames$playerID
ids_to_check <- goalie_summary$playerID
ids_to_add <- setdiff(ids_to_check,ids)
if(length(ids_to_add)>0){
  for(i in 1:length(ids_to_add)){
    newplayer <- player(ids_to_add[i])
    playernames <- rbind(playernames,newplayer)
  }
  dbWriteTable(con, name = "player_data", value = playernames, row.names = FALSE, overwrite = TRUE)
}
goalie_summary <- playernames %>% left_join(goalie_summary, by="playerID")
goalie_summary <- na.omit(goalie_summary)
dbWriteTable(con, name = "goalie_stats__for_sim", value = goalie_summary, row.names = FALSE, overwrite = TRUE)


################################################################################

#### Season Sim ################################################################
player_summary <- dbReadTable(con,"player_stats__for_sim")
goalie_summary <- dbReadTable(con,"goalie_stats__for_sim")
player_stats <- dbReadTable(con,"player_stats_24_25")
goalie_data <- dbReadTable(con,"goalie_stats_24_25")

player_ovr <- player_summary %>% filter(toi>toi_threshold_even) %>% select(playerID,full_name,team,position,OVR,toi) %>% drop_na()
goalie_ovr <- goalie_summary %>% mutate(toi = gp*20) %>% select(playerID,full_name,team,position,GOA,toi) %>% rename(OVR = GOA) %>% drop_na()
all_players <- bind_rows(player_ovr,goalie_ovr)

teams <- unique(schedule_24_25$home_abbr)
rosters <- team_roster(teams[1])
for(i in 2:length(teams)){
  print(i)
  roster_to_add <- team_roster(teams[i])
  rosters <- bind_rows(rosters,roster_to_add)
}
rosters <- rosters %>% select(id,team) %>% rename(playerID = id)

player_eligibility <- player_stats %>% mutate(eligibility = ifelse(toi >= toi_threshold_even,1,0)) %>% select(playerID,eligibility)
goalie_eligibility <- goalie_data %>% mutate(eligibility = ifelse(gp >= goalie_games,1,0)) %>% select(playerID,eligibility)
eligibility <- bind_rows(player_eligibility,goalie_eligibility)
all_players <- all_players %>% select(playerID,OVR,toi)

rosters_to_use <- rosters %>%
  left_join(eligibility,by="playerID") %>%
  left_join(all_players,by="playerID")
rosters_to_use <- na.omit(rosters_to_use)

byteam <- rosters_to_use %>%
  filter(eligibility == 1) %>%
  group_by(team) %>%
  summarize(OVR = weighted.mean(OVR,toi),
            .groups = "drop") %>%
  mutate(team_strength = log(OVR)*3) %>%
  select(-OVR) %>%
  arrange(desc(team_strength))

standings <- team_standings()
standings <- standings[-c(10:15)]
standings <- standings %>%
  mutate(playoff = 0,
         division_place = 0,
         league_place = 0,
         lose_1st_round = 0,
         division_final = 0,
         conference_final = 0,
         cup_final = 0,
         cup_winner = 0,
         sim_num = 0
  )
n <- 50000
playoff_sim <- function(x,y){
  u <- 100
  home_team <- x
  away_team <- y
  
  home_players <- as.numeric(byteam[byteam$team == home_team, 'team_strength'])
  away_players <- as.numeric(byteam[byteam$team == away_team, 'team_strength'])
  
  home_goals <- rnorm(u,mean=home_players,8) + 0.2
  away_goals <- rnorm(u,mean=away_players,8)
  
  home_win <- 0
  away_win <- 0
  
  for (a in 1:u){
    ifelse (home_goals[a]>away_goals[a],
            home_win <- as.numeric(home_win) + 1,
            away_win <- as.numeric(away_win) + 1)
  }
  
  home_win <- as.numeric(home_win) / u
  away_win <- as.numeric(away_win) / u
  
  winning_team <- ifelse(home_win>0.5,x,y)
  losing_team <- ifelse(home_win>0.5,y,x)
  series_result <- c(winning_team,losing_team)
  return(series_result)
}
sim_history <- standings
sim_history <- sim_history[0,]
playoff_history <- data.frame(sim=0,series="",winner="",loser="")
playoff_history <- playoff_history[0,]
schedule_24_25 <- schedule_24_25 %>% filter(is.na(home_score))

for(i in 1:n){
  print(i)
  empty_schedule <- schedule_24_25
  empty_standings <- standings
  # Run game sim on each game #
  for(j in 1:nrow(empty_schedule)){
    away_strength <- as.numeric(byteam[byteam$team == empty_schedule$away_abbr[j], 'team_strength'])
    home_strength <- as.numeric(byteam[byteam$team == empty_schedule$home_abbr[j], 'team_strength'])
    empty_schedule$away_score[j] <- rnorm(1,away_strength+3.15,2)
    empty_schedule$home_score[j] <- rnorm(1,home_strength+3.15,2) + 0.23
  }
  # Calculate standings #
  for(k in 1:nrow(empty_standings)){
    empty_standings$gamesPlayed[k] <- as.numeric(length(which(empty_schedule$away_abbr == empty_standings$teamAbbrev[k]))) + 
      as.numeric(length(which(empty_schedule$home_abbr == empty_standings$teamAbbrev[k]))) + empty_standings$gamesPlayed[k]
    empty_standings$wins[k] <- as.numeric(length(which(empty_schedule$away_abbr == empty_standings$teamAbbrev[k] & empty_schedule$away_score > empty_schedule$home_score))) +
      as.numeric(length(which(empty_schedule$home_abbr == empty_standings$teamAbbrev[k] & empty_schedule$home_score > empty_schedule$away_score))) + empty_standings$wins[k]
    empty_standings$losses[k] <- as.numeric(length(which(empty_schedule$away_abbr == empty_standings$teamAbbrev[k] & empty_schedule$away_score < empty_schedule$home_score & (empty_schedule$home_score-empty_schedule$away_score) > 1))) +
      as.numeric(length(which(empty_schedule$home_abbr == empty_standings$teamAbbrev[k] & empty_schedule$home_score < empty_schedule$away_score & (empty_schedule$away_score-empty_schedule$home_score) > 1))) + empty_standings$losses[k]
    empty_standings$otLosses[k] <- as.numeric(length(which(empty_schedule$away_abbr == empty_standings$teamAbbrev[k] & empty_schedule$away_score < empty_schedule$home_score & (empty_schedule$home_score-empty_schedule$away_score) < 1))) +
      as.numeric(length(which(empty_schedule$home_abbr == empty_standings$teamAbbrev[k] & empty_schedule$home_score < empty_schedule$away_score & (empty_schedule$away_score-empty_schedule$home_score) < 1))) + empty_standings$otLosses[k]
    empty_standings$points[k] <- (empty_standings$wins[k] * 2) + empty_standings$otLosses[k]
  }
  empty_standings <- empty_standings %>% 
    arrange(desc(points),desc(wins))
  empty_standings <- empty_standings %>%
    mutate(league_place = row_number(),
           sim_num = i) %>%
    arrange(divisionName,desc(points),desc(wins)) %>%
    group_by(divisionName) %>%
    mutate(division_place = rank(-points,ties.method = "first")) %>%
    ungroup() %>%
    arrange(desc(points),desc(wins))
  
  # Playoff sim #
  atlantic_1 <- empty_standings[empty_standings$divisionName=="Atlantic",]$teamAbbrev[1]
  atlantic_2 <- empty_standings[empty_standings$divisionName=="Atlantic",]$teamAbbrev[2]
  atlantic_3 <- empty_standings[empty_standings$divisionName=="Atlantic",]$teamAbbrev[3]
  metro_1 <- empty_standings[empty_standings$divisionName=="Metropolitan",]$teamAbbrev[1]
  metro_2 <- empty_standings[empty_standings$divisionName=="Metropolitan",]$teamAbbrev[2]
  metro_3 <- empty_standings[empty_standings$divisionName=="Metropolitan",]$teamAbbrev[3]
  east_playoffs <- empty_standings[empty_standings$divisionName == "Atlantic"|empty_standings$divisionName == "Metropolitan",]$teamAbbrev
  east_playoffs <- strsplit(east_playoffs,"  ")
  east_divisionNames <- c(atlantic_1,atlantic_2,atlantic_3,metro_1,metro_2,metro_3)
  east_divisionNames <- strsplit(east_divisionNames,"  ")
  east <- setdiff(east_playoffs,east_divisionNames)
  east_1 <- as.character(east[1])
  east_2 <- as.character(east[2])
  central_1 <- empty_standings[empty_standings$divisionName=="Central",]$teamAbbrev[1]
  central_2 <- empty_standings[empty_standings$divisionName=="Central",]$teamAbbrev[2]
  central_3 <- empty_standings[empty_standings$divisionName=="Central",]$teamAbbrev[3]
  pacific_1 <- empty_standings[empty_standings$divisionName=="Pacific",]$teamAbbrev[1]
  pacific_2 <- empty_standings[empty_standings$divisionName=="Pacific",]$teamAbbrev[2]
  pacific_3 <- empty_standings[empty_standings$divisionName=="Pacific",]$teamAbbrev[3]
  west_playoffs <- empty_standings[empty_standings$divisionName == "Central"|empty_standings$divisionName == "Pacific",]$teamAbbrev
  west_playoffs <- strsplit(west_playoffs,"  ")
  west_divisionNames <- c(central_1,central_2,central_3,pacific_1,pacific_2,pacific_3)
  west_divisionNames <- strsplit(west_divisionNames,"  ")
  west <- setdiff(west_playoffs,west_divisionNames)
  west_1 <- as.character(west[1])
  west_2 <- as.character(west[2])
  
  empty_standings[empty_standings$teamAbbrev == atlantic_1,"playoff"] <- 1
  empty_standings[empty_standings$teamAbbrev == atlantic_2,"playoff"] <- 1
  empty_standings[empty_standings$teamAbbrev == atlantic_3,"playoff"] <- 1
  empty_standings[empty_standings$teamAbbrev == metro_1,"playoff"] <- 1
  empty_standings[empty_standings$teamAbbrev == metro_2,"playoff"] <- 1
  empty_standings[empty_standings$teamAbbrev == metro_3,"playoff"] <- 1
  empty_standings[empty_standings$teamAbbrev == east_1,"playoff"] <- 1
  empty_standings[empty_standings$teamAbbrev == east_2,"playoff"] <- 1
  empty_standings[empty_standings$teamAbbrev == central_1,"playoff"] <- 1
  empty_standings[empty_standings$teamAbbrev == central_2,"playoff"] <- 1
  empty_standings[empty_standings$teamAbbrev == central_3,"playoff"] <- 1
  empty_standings[empty_standings$teamAbbrev == pacific_1,"playoff"] <- 1
  empty_standings[empty_standings$teamAbbrev == pacific_2,"playoff"] <- 1
  empty_standings[empty_standings$teamAbbrev == pacific_3,"playoff"] <- 1
  empty_standings[empty_standings$teamAbbrev == west_1,"playoff"] <- 1
  empty_standings[empty_standings$teamAbbrev == west_2,"playoff"] <- 1
  
  ifelse(empty_standings[empty_standings$teamAbbrev == atlantic_1, "league_place"]<empty_standings[empty_standings$teamAbbrev == metro_1,"league_place"],
         atlanticplayoff_1 <- playoff_sim(atlantic_1,east_2),
         atlanticplayoff_1 <- playoff_sim(atlantic_1,east_1))
  atlanticplayoff_2 <- playoff_sim(atlantic_2,atlantic_3)
  ifelse(empty_standings[empty_standings$teamAbbrev == metro_1, "league_place"]<empty_standings[empty_standings$teamAbbrev == atlantic_1,"league_place"],
         metroplayoff_1 <- playoff_sim(metro_1,east_2),
         metroplayoff_1 <- playoff_sim(metro_1,east_1))
  metroplayoff_2 <- playoff_sim(metro_2,metro_3)
  ifelse(empty_standings[empty_standings$teamAbbrev == central_1, "league_place"]<empty_standings[empty_standings$teamAbbrev == pacific_1,"league_place"],
         centralplayoff_1 <- playoff_sim(central_1,west_2),
         centralplayoff_1 <- playoff_sim(central_1,west_1))
  centralplayoff_2 <- playoff_sim(central_2,central_3)
  ifelse(empty_standings[empty_standings$teamAbbrev == pacific_1, "league_place"]<empty_standings[empty_standings$teamAbbrev == central_1,"league_place"],
         pacificplayoff_1 <- playoff_sim(pacific_1,west_2),
         pacificplayoff_1 <- playoff_sim(pacific_1,west_1))
  pacificplayoff_2 <- playoff_sim(pacific_2,pacific_3)
  
  empty_standings[empty_standings$teamAbbrev == atlanticplayoff_1[2],"lose_1st_round"] <- 1
  empty_standings[empty_standings$teamAbbrev == atlanticplayoff_2[2],"lose_1st_round"] <- 1
  empty_standings[empty_standings$teamAbbrev == metroplayoff_1[2],"lose_1st_round"] <- 1
  empty_standings[empty_standings$teamAbbrev == metroplayoff_2[2],"lose_1st_round"] <- 1
  empty_standings[empty_standings$teamAbbrev == centralplayoff_1[2],"lose_1st_round"] <- 1
  empty_standings[empty_standings$teamAbbrev == centralplayoff_2[2],"lose_1st_round"] <- 1
  empty_standings[empty_standings$teamAbbrev == pacificplayoff_1[2],"lose_1st_round"] <- 1
  empty_standings[empty_standings$teamAbbrev == pacificplayoff_2[2],"lose_1st_round"] <- 1
  
  ifelse (atlanticplayoff_1[1]==atlantic_1,
          atlanticplayoff_final <- playoff_sim(atlanticplayoff_1[1],atlanticplayoff_2[1]),
          atlanticplayoff_final <- playoff_sim(atlanticplayoff_2[1],atlanticplayoff_1[1]))
  ifelse (metroplayoff_1[1]==metro_1,
          metroplayoff_final <- playoff_sim(metroplayoff_1[1],metroplayoff_2[1]),
          metroplayoff_final <- playoff_sim(metroplayoff_2[1],metroplayoff_1[1]))
  ifelse (centralplayoff_1[1]==central_1,
          centralplayoff_final <- playoff_sim(centralplayoff_1[1],centralplayoff_2[1]),
          centralplayoff_final <- playoff_sim(centralplayoff_2[1],centralplayoff_1[1]))
  ifelse (pacificplayoff_1[1]==pacific_1,
          pacificplayoff_final <- playoff_sim(pacificplayoff_1[1],pacificplayoff_2[1]),
          pacificplayoff_final <- playoff_sim(pacificplayoff_2[1],pacificplayoff_1[1]))
  
  empty_standings[empty_standings$teamAbbrev == atlanticplayoff_final[2],"division_final"] <- 1
  empty_standings[empty_standings$teamAbbrev == metroplayoff_final[2],"division_final"] <- 1
  empty_standings[empty_standings$teamAbbrev == centralplayoff_final[2],"division_final"] <- 1
  empty_standings[empty_standings$teamAbbrev == pacificplayoff_final[2],"division_final"] <- 1
  
  ifelse ((empty_standings[empty_standings$teamAbbrev == atlanticplayoff_final[1],"league_place"] < empty_standings[empty_standings$teamAbbrev == metroplayoff_final[1],"league_place"]),
          eastplayoff_final <- playoff_sim(atlanticplayoff_final[1],metroplayoff_final[1]),
          eastplayoff_final <- playoff_sim(metroplayoff_final[1],atlanticplayoff_final[1]))
  ifelse ((empty_standings[empty_standings$teamAbbrev == centralplayoff_final[1],"league_place"] < empty_standings[empty_standings$teamAbbrev == pacificplayoff_final[1],"league_place"]),
          westplayoff_final <- playoff_sim(centralplayoff_final[1],pacificplayoff_final[1]),
          westplayoff_final <- playoff_sim(pacificplayoff_final[1],centralplayoff_final[1]))
  
  empty_standings[empty_standings$teamAbbrev == eastplayoff_final[2],"conference_final"] <- 1
  empty_standings[empty_standings$teamAbbrev == westplayoff_final[2],"conference_final"] <- 1
  
  ifelse ((empty_standings[empty_standings$teamAbbrev == eastplayoff_final[1],"league_place"] < empty_standings[empty_standings$teamAbbrev == westplayoff_final[1],"league_place"]),
          cupfinal <- playoff_sim(eastplayoff_final[1],westplayoff_final[1]),
          cupfinal <- playoff_sim(westplayoff_final[1],eastplayoff_final[1]))
  empty_standings[empty_standings$teamAbbrev == cupfinal[1],"cup_winner"] <- 1
  empty_standings[empty_standings$teamAbbrev == cupfinal[2],"cup_final"] <- 1
  
  # Add to overall tracker #
  sim_history <- bind_rows(sim_history,empty_standings)
  atl_1 <- unlist(str_split(atlanticplayoff_1," "))
  atl_1 <- data.frame(sim=i,series=deparse(substitute(atlanticplayoff_1)),winner=atl_1[c(TRUE,FALSE)], loser=atl_1[c(FALSE,TRUE)])
  atl_2 <- unlist(str_split(atlanticplayoff_2," "))
  atl_2 <- data.frame(sim=i,series=deparse(substitute(atlanticplayoff_2)),winner=atl_2[c(TRUE,FALSE)], loser=atl_2[c(FALSE,TRUE)])
  met_1 <- unlist(str_split(metroplayoff_1," "))
  met_1 <- data.frame(sim=i,series=deparse(substitute(metroplayoff_1)),winner=met_1[c(TRUE,FALSE)], loser=met_1[c(FALSE,TRUE)])
  met_2 <- unlist(str_split(metroplayoff_2," "))
  met_2 <- data.frame(sim=i,series=deparse(substitute(metroplayoff_2)),winner=met_2[c(TRUE,FALSE)], loser=met_2[c(FALSE,TRUE)])
  cen_1 <- unlist(str_split(centralplayoff_1," "))
  cen_1 <- data.frame(sim=i,series=deparse(substitute(centralplayoff_1)),winner=cen_1[c(TRUE,FALSE)], loser=cen_1[c(FALSE,TRUE)])
  cen_2 <- unlist(str_split(centralplayoff_2," "))
  cen_2 <- data.frame(sim=i,series=deparse(substitute(centralplayoff_2)),winner=cen_2[c(TRUE,FALSE)], loser=cen_2[c(FALSE,TRUE)])
  pac_1 <- unlist(str_split(pacificplayoff_1," "))
  pac_1 <- data.frame(sim=i,series=deparse(substitute(pacificplayoff_1)),winner=pac_1[c(TRUE,FALSE)], loser=pac_1[c(FALSE,TRUE)])
  pac_2 <- unlist(str_split(pacificplayoff_2," "))
  pac_2 <- data.frame(sim=i,series=deparse(substitute(pacificplayoff_2)),winner=pac_2[c(TRUE,FALSE)], loser=pac_2[c(FALSE,TRUE)])
  
  atl_fin <- unlist(str_split(atlanticplayoff_final," "))
  atl_fin <- data.frame(sim=i,series=deparse(substitute(atlanticplayoff_final)),winner=atl_fin[c(TRUE,FALSE)], loser=atl_fin[c(FALSE,TRUE)])
  met_fin <- unlist(str_split(metroplayoff_final," "))
  met_fin <- data.frame(sim=i,series=deparse(substitute(metroplayoff_final)),winner=met_fin[c(TRUE,FALSE)], loser=met_fin[c(FALSE,TRUE)])
  cen_fin <- unlist(str_split(centralplayoff_final," "))
  cen_fin <- data.frame(sim=i,series=deparse(substitute(centralplayoff_final)),winner=cen_fin[c(TRUE,FALSE)], loser=cen_fin[c(FALSE,TRUE)])
  pac_fin <- unlist(str_split(pacificplayoff_final," "))
  pac_fin <- data.frame(sim=i,series=deparse(substitute(atlanticplayoff_final)),winner=pac_fin[c(TRUE,FALSE)], loser=pac_fin[c(FALSE,TRUE)])
  
  east_fin <- unlist(str_split(eastplayoff_final," "))
  east_fin <- data.frame(sim=i,series=deparse(substitute(eastplayoff_final)),winner=east_fin[c(TRUE,FALSE)], loser=east_fin[c(FALSE,TRUE)])
  west_fin <- unlist(str_split(westplayoff_final," "))
  west_fin <- data.frame(sim=i,series=deparse(substitute(westplayoff_final)),winner=west_fin[c(TRUE,FALSE)], loser=west_fin[c(FALSE,TRUE)])
  
  cup_fin <- unlist(str_split(cupfinal," "))
  cup_fin <- data.frame(sim=i,series=deparse(substitute(cupfinal)),winner=cup_fin[c(TRUE,FALSE)], loser=cup_fin[c(FALSE,TRUE)])
  
  playoff_history <- rbind(playoff_history,atl_1,atl_2,met_1,met_2,cen_1,cen_2,pac_1,pac_2,atl_fin,met_fin,cen_fin,pac_fin,east_fin,west_fin,cup_fin)
}

sim_summary <- sim_history %>%
  group_by(teamAbbrev,conferenceName,divisionName) %>%
  summarize(
    points = round(mean(points),2),
    playoff = round(sum(playoff)/n*100,1),
    president_trophy = round(sum(league_place == 1)/n*100,2),
    first_round = round(sum(lose_1st_round)/n*100,2),
    division_final = round(sum(division_final)/n*100,2),
    conference_final = round(sum(conference_final)/n*100,2),
    cup_final = round(sum(cup_final)/n*100,2),
    cup_winner = round(sum(cup_winner)/n*100,1),
    .groups = "drop"
  ) %>%
  arrange(desc(points)) %>%
  mutate(logo = glue(
    '<img height=50 src="https://assets.nhle.com/logos/nhl/svg/{teamAbbrev}_light.svg"></img>'
  ))

dbWriteTable(con, name = "inseason_sim_summary", value = sim_summary, row.names = FALSE, overwrite = TRUE)
dbWriteTable(con, name = "inseason_sim_history", value = sim_history, row.names = FALSE, overwrite = TRUE)
dbWriteTable(con, name = "inseason_playoff_history", value = playoff_history, row.names = FALSE, overwrite = TRUE)

################################################################################

#### Player Card Data ##########################################################

################################################################################

#### Team Data #################################################################
pbp <- dbReadTable(con,"pbp_24_25")

team_toi <- toi_teams(pbp)

team_stats <- team_data(pbp,"all")
team_stats <- team_toi %>%
  select(team,toi=toi_all) %>%
  left_join(team_stats,by="team") %>%
  mutate("gf%" = gf/(gf+ga),"xgf%" = xgf/(xgf+xga),"sf%" = sf/(sf+sa),"cf%" = cf/(cf+ca),"ff%" = ff/(ff+fa),"gf/60" = (gf/toi)*60,"ga/60" = (ga/toi)*60,
         "xgf/60" = (xgf/toi)*60,"xga/60" = (xga/toi)*60,"cf/60" = (cf/toi)*60,"ca/60" = (ca/toi)*60
  )
dbWriteTable(con, name = "team_stats_all_24_25", value = team_stats, row.names = FALSE, overwrite = TRUE)

team_stats_5v5 <- team_data(pbp,"even")
team_stats_5v5 <- team_toi %>%
  select(team,toi=toi_5v5) %>%
  left_join(team_stats_5v5,by="team") %>%
  mutate("gf%" = gf/(gf+ga),"xgf%" = xgf/(xgf+xga),"sf%" = sf/(sf+sa),"cf%" = cf/(cf+ca),"ff%" = ff/(ff+fa),"gf/60" = (gf/toi)*60,"ga/60" = (ga/toi)*60,
         "xgf/60" = (xgf/toi)*60,"xga/60" = (xga/toi)*60,"cf/60" = (cf/toi)*60,"ca/60" = (ca/toi)*60
  )
dbWriteTable(con, name = "team_stats_5v5_24_25", value = team_stats_5v5, row.names = FALSE, overwrite = TRUE)

################################################################################

dbDisconnect(con)

