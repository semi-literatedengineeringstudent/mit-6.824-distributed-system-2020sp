package raft

var leader_role int = 0
var candidate_role int = 1
var follower_role int = 2

var not_voted int = -1

var sentinel_index int = 0

var default_start_term int = 0

var election_time_out_lower_bound_millisecond int = 500
var election_time_out_range_millisecond int = 1000

var follower_loop_wait_time_millisecond int = 50
var leader_heartbeat_millisecond int = 100
