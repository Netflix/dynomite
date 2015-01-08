/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#include "dyn_core.h"
#include "dyn_conf.h"

static uint8_t tracking_level = 0;
static uint32_t max_tokens = CONF_DEFAULT_CONN_MAX_AVAL_TOKENS;  //max msgs can be seen through a conn
static uint32_t conn_msg_rate = CONF_DEFAULT_CONN_MSG_RATE;           //conn msgs per sec


uint8_t get_tracking_level(void)
{
    return tracking_level;
}

void set_tracking_level(uint8_t level)
{
    tracking_level = level;
}

uint32_t max_allowable_rate(void)
{
	return max_tokens;
}


void set_max_allowable_rate(uint32_t rate)
{
	max_tokens = rate;
}


uint32_t tokens_earned_per_sec(void)
{
   return conn_msg_rate;
}

void set_tokens_earned_per_sec(uint32_t tokens_per_sec)
{
	conn_msg_rate = tokens_per_sec;
}
