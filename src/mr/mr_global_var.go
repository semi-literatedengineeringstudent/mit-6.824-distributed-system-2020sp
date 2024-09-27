package mr

var map_task int = 0
var reduce_task int = 1
var none_task int = 2
var exit_task int = 3

var worker_idle int = 0
var worker_complete int = 1

var task_unscheduled int = 0
var task_in_progress int = 1
var task_completed int = 2

var master_wait_time_second int = 10

var worker_request_interval_millisecond int = 500
