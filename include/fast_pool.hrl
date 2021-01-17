-record(pool_info, {
        poolname, 
        cur_size = 0, 
        max_size = 0, 
        last_used_pid, % used for roundrobin when all connections are working
        is_busy = false, % whether all connections busy
        mod, % for start
        stop_mf, % for stop
        queue_limit % max load for one connection
    }).

-record(conn_info, {
        pid,
        load = 0, % how many client processes are waiting for query result
        last_used_timestamp = erlang:system_time(second),
        is_sync = 0 % whether this connection is acquired by a sync query
    }).

-record(pool_use_reg, {
        pid_and_ref, % client process pid and a unique reference
        poolname, 
        connection_pid, 
        is_sync, 
        timestamp
    }).
