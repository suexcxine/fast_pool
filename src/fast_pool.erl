-module(fast_pool).
-include("fast_pool.hrl").
%%% pick idle connection first
%%% make new connection lazily
%%% when no idle connection and pool not full, make a new connection
%%% when no idle connection and pool is full, roundrobin old connections
%%% auto recycle connections which is idle for more than configured seconds
%%% auto recycling happens periodically(configurable)
-export([query/2, query/4, adjust_maxsize/2]).

query(Poolname, F) ->
  query(Poolname, F, false, 0).

query(_Poolname, _F, _IsSync = false, RetriedTimes) when RetriedTimes > 5 ->
  error(<<"query retried too many times">>);
query(_Poolname, _F, _IsSync = true, RetriedTimes) when RetriedTimes > 60 ->
  error(<<"sync query retried too many times">>);
query(Poolname, F, IsSync, RetriedTimes) ->
  case get_one_pid(Poolname, IsSync) of
    {_QueueLimit, false} ->
      % not found, try again later
      timer:sleep(50),
      query(Poolname, F, IsSync, RetriedTimes + 1);
    {QueueLimit, Pid} ->
      case do_query(Poolname, Pid, F, QueueLimit, IsSync) of
        {ok, Reply} ->
          Reply;
        false ->
          timer:sleep(50),
          query(Poolname, F, IsSync, RetriedTimes + 1)
      end
  end.

get_one_pid(Poolname, IsSync) ->
  [PoolInfo] = ets:lookup(pools, Poolname),
  #pool_info{
     poolname = Poolname, 
     cur_size = CurSize, 
     max_size = MaxSize, 
     last_used_pid = LastUsedPid, 
     is_busy = IsBusy, 
     queue_limit = QueueLimit} = PoolInfo,
  case IsBusy of
    true ->
      % no idle connection according to busy flag
      % maybe busy flag has been updated now, but no problem
      Pid = get_one_pid_on_busy(Poolname, CurSize, MaxSize, LastUsedPid, IsSync),
      {QueueLimit, Pid};
    false ->
      MS = [{#conn_info{pid = '$1', load = 0, last_used_timestamp = '_', is_sync = 0}, [], ['$1']}],
      case ets:select(Poolname, MS, 1) of
        {[Pid], _Continuation} ->
          {QueueLimit, Pid};
        _ ->
          % no idle connection according to connection registry
          % we need to update busy flag to avoid meaningless ets:select next time
          ets:update_element(pools, Poolname, {#conn_info.is_sync, true}),
          Pid = get_one_pid_on_busy(Poolname, CurSize, MaxSize, LastUsedPid, IsSync),
          {QueueLimit, Pid}
      end
  end.

get_one_pid_on_busy(Poolname, CurSize, MaxSize, LastUsedPid, IsSync) ->
  HardMaxSize = case IsSync of
    true ->
      % TODO, add a config for sync max size, since sync query need more connections
      MaxSize * 3; 
    false ->
      MaxSize
  end,
  CanSpawn = case CurSize < HardMaxSize of
    true ->
      % since other processes maybe also doing this
      % we need to check again
      case ets:update_counter(pools, Poolname, {#pool_info.cur_size, 1}) =< HardMaxSize of
        true ->
          true;
        false ->
          % exceeded, release
          ets:update_counter(pools, Poolname, {#pool_info.cur_size, -1}),
          false
      end;
    false ->
      false
  end,
  case CanSpawn of
    true ->
      start_new_conn(Poolname);
    false ->
      case IsSync of
        true ->
          % sync query can't use connections being used
          false;
        false ->
          case next_pid(Poolname, LastUsedPid) of
            Pid when is_pid(Pid) ->
              ets:update_element(pools, Poolname, {#pool_info.last_used_pid, Pid}),
              Pid;
            _ ->
              false
          end
      end
  end.

next_pid(Poolname, Pid) when is_pid(Pid) ->
  try ets:next(Poolname, Pid) of
    NextPid when is_pid(NextPid) ->
      NextPid;
    _ ->
      % '$end_of_table'
      ets:first(Poolname)
  catch
    _:_ ->
      % maybe pid is gone, not a valid key anymore
      ets:first(Poolname)
  end;
next_pid(Poolname, _Pid) ->
  % maybe Pid is 0, undefined, or '$end_of_table'
  ets:first(Poolname).

% confirm connection usable, query, and release connection
do_query(Poolname, Pid, F, QueueLimit, IsSync) ->
  Updates = if IsSync -> [{#conn_info.load, 1}, {#conn_info.is_sync, 1}]; 
               true -> [{#conn_info.load, 1}] 
            end,
  Now = erlang:system_time(second),
  PoolUseRegKey = {self(), make_ref()},
  try is_checkout_result_ok(ets:update_counter(Poolname, Pid, Updates), Poolname, Pid, QueueLimit) of
    true ->
      % we don't consider the case in which pid is dead, just leave it to application layer
      ets:update_element(Poolname, Pid, {#conn_info.last_used_timestamp, Now}),
      % register, in order to avoid client process killed before after clause execute
      % if client process died before registration, we leave it to sweeper
      ets:insert(pool_use_reg, #pool_use_reg{pid_and_ref = PoolUseRegKey, 
                                             poolname = Poolname, 
                                             connection_pid = Pid, 
                                             is_sync = IsSync, 
                                             timestamp = Now}),
      Reply = F(Pid),
      {ok, Reply};
    _ ->
      false
  after
    % release load counter
    RevUpdates = if IsSync -> [{#conn_info.load, -1}, {#conn_info.is_sync, -1, 0, 0}]; 
                    true -> [{#conn_info.load, -1}] 
                 end,
    % if it's idle now, namely it's not busy, update flag
    hd(ets:update_counter(Poolname, Pid, RevUpdates)) == 0 andalso
      ets:update_element(pools, Poolname, {#pool_info.is_busy, false}),
    ets:delete(pool_use_reg, PoolUseRegKey)
  end.

is_checkout_result_ok([Load], Poolname, Pid, QueueLimit) ->
  case Load > 0 andalso Load =< QueueLimit of
    true ->
      % non-sync query can't use connections checked out by sync queries
      % it's enough to use a ets:lookup
      % since if a sync query follows immediately after this lookup
      % it will find load > 1, so it's ok
      case ets:lookup(Poolname, Pid) of
        [#conn_info{is_sync = 0}] -> true;
        _ -> false
      end;
    false ->
      false
  end;
is_checkout_result_ok([Load, SyncCount], _Poolname, _Pid, _QueueLimit) ->
  % we need to make sure this is the only checkout for sync query
  Load =:= 1 andalso SyncCount =:= 1.


start_new_conn(Poolname) ->
  % start a new connection
  {ok, Pid} = fast_pool_sup:start_child(Poolname),
  % register
  ets:insert(Poolname, #conn_info{pid = Pid}),
  gen_server:cast(fast_pool_sweeper, {register_monitor, Poolname, Pid}),
  Pid.

adjust_maxsize(Poolname, NewMaxSize) when NewMaxSize > 0 ->
  ets:update_element(pools, Poolname, {#pool_info.max_size, NewMaxSize}).
