package shardkv

var killed_kvserver_busywait_avoid_time_millisecond int = 1000

var kvserver_loop_wait_time_millisecond int = 5

var empty_string string = ""

var default_sentinel_index int = 0

var invalid_index int = -1

var default_start_term int = 0

var invalid_term int = 0

var invalid_leader int = -1

var leader_role int = 0
var candidate_role int = 1
var follower_role int = 2

var client_wait_time_millisecond int = 2000

var client_wait_time_loop_millisecond int = 100