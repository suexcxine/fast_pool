-module(fast_pool_sweeper).
%%% 连接池自动回收器
%%% 定时回收没有被用到的连接
-include_lib("stdlib/include/ms_transform.hrl").
-include("fast_pool.hrl").
-behaviour(gen_server).
-export([clean_all_conn/1]).
-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

% try to recycle every 60 seconds
-define(SWEEP_INTERVAL, 60).

% recycle idle connections which is idle for more than 120 seconds
-define(SWEEP_TIMEOUT, 120).

% recycle zombie connections which is idle for more than 600 seconds
% maybe, client process killed before release caused these zombie connections
-define(ZOMBIE_SWEEP_TIMEOUT, 600).

% client process registration expires in 20 seconds
-define(POOL_REG_EXPIRE_SECONDS, 20).

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

% DANGER! 关掉全部连接重新再来, 运维使用
clean_all_conn(Poolname) ->
  case ets:lookup(pools, Poolname) of
    [#pool_info{poolname = Poolname, stop_mf = StopMF}] ->
      Pids = [Pid || #conn_info{pid = Pid} <- ets:tab2list(Poolname)],
      {clean_all_conn(Poolname, Pids, StopMF, 0), length(Pids)};
  
    _ ->
      no_such_pool
  end.

clean_all_conn(_Poolname, LeftPids, _StopMF, RetriedTimes) when RetriedTimes > 3 ->
  {left, LeftPids, length(LeftPids)};

clean_all_conn(_Poolname, [], _StopMF, _RetriedTimes) ->
  ok;

clean_all_conn(Poolname, Pids, StopMF, RetriedTimes) ->
    LeftPids = [Pid || Pid <- Pids, not do_clean_one_conn(Poolname, Pid, 0, StopMF)],
    timer:sleep(100),
    clean_all_conn(Poolname, LeftPids, StopMF, RetriedTimes + 1).

% ---------------------------------
%  GenServer Callbacks
% ---------------------------------

init(_) ->
  process_flag(trap_exit, true),
  start_timer(),
  {ok, #{}}.

handle_call(_Req, _From, State) ->
  {reply, ok, State}.

handle_cast({register_monitor, Poolname, Pid}, State) ->
  monitor(process, Pid),
  ets:insert(pool_reverse, {Pid, Poolname}),
  {noreply, State};
handle_cast(_, State) ->
  {noreply, State}.

handle_info({'DOWN', _Ref, process, Pid, _}, State) ->
  % a connection pid died
  try
    case ets:lookup(pool_reverse, Pid) of
      [{Pid, Poolname}] ->
        ets:delete(pool_reverse, Pid),
        ets:update_counter(pools, Poolname, {#pool_info.cur_size, -1}),
        % 这一行还要, 虽然在 stop 之前已经删除了,
        % 但是如果是异常 DOWN 的情况下, 那里走不到, 还得靠这里保底
        ets:delete(Poolname, Pid),
        ok;
      _ ->
        ok
    end,
    {noreply, State}
  catch
    _:Reason:StackTrace ->        
      Log = io_lib:format("Pool cleaning, error:\n~s\nstacktrace:\n~s", [Reason, StackTrace]), 
      error_logger:error_msg(Log),
      {noreply, State}
  end;
handle_info(sweep, State) ->
  SweepTimeout = get_sweep_timeout(),
  Now = erlang:system_time(second),
  % close inactive connections to save resources
  lists:foreach(fun(Pool) -> clean_one_pool(Pool, Now, SweepTimeout) end, ets:tab2list(pools)),
  % sweep expired registrations
  clean_use_reg(Now),
  start_timer(),
  {noreply, State};
handle_info(_, State) ->
  {noreply, State}.

terminate(_reason, _) ->
  ok.

% ---------------------------------
%  Internal Functions
% ---------------------------------

clean_one_pool(#pool_info{poolname = Poolname, cur_size = CurSize, max_size = MaxSize, 
    is_busy = IsBusy, mod = Mod, stop_mf = StopMF}, Now, SweepTimeout) ->
  lists:foreach(fun(Conn) -> clean_one_conn(Conn, Poolname, StopMF, Now, SweepTimeout) end, 
    ets:tab2list(Poolname)),
  % try to auto adapt work load, if busy, open new connections by 10% 
  case IsBusy of
    true ->
      SpawnCount = ceil(CurSize / 10),
      % no more than 3 times of max_pool_size
      SpawnCount = max(min(SpawnCount, MaxSize * 3 - CurSize), 0),
      lists:foreach(fun(_) -> fast_pool:start_new_conn(Poolname, Mod) end, lists:seq(1, SpawnCount)),
      ets:update_counter(pools, Poolname, {#pool_info.cur_size, SpawnCount});
    false ->
      ok
  end,

  % resync current size info in pools ets
  % because it's sometimes inaccurate
  % for instance, client process could be killed after update_counter(checkout)
  % since we don't need it to be strictly accurate, so resync periodically like this should be fine
  ets:update_element(pools, Poolname, {#pool_info.cur_size, ets:info(Poolname, size)}),
  ok.

clean_one_conn(#conn_info{pid = Pid, load = 0, last_used_timestamp = LastUsedTimestamp, is_sync = 0}, 
    Poolname, StopMF, Now, SweepTimeout) ->
  case Now - LastUsedTimestamp >= SweepTimeout of
    true ->
      do_clean_one_conn(Poolname, Pid, 0, StopMF);
    false ->
      ok
  end;
clean_one_conn(#conn_info{pid = Pid, load = N, last_used_timestamp = LastUsedTimestamp}, 
    Poolname, StopMF, Now, _SweepTimeout) when Now - LastUsedTimestamp >= ?ZOMBIE_SWEEP_TIMEOUT ->
  % although its load is not zero, it's too long
  % we have a reason to suspect that there is a problem
  do_clean_one_conn(Poolname, Pid, N, StopMF);
clean_one_conn(_, _, _, _, _) ->
  ok.

do_clean_one_conn(Poolname, Pid, N, {StopM, StopF}) ->
  % 这么做的意思是确保没有其他 actor 在用, 相当于标记为要回收
  % 这里减 100 万是怕有几个 actor checkout 时都 + 1 结果加到 > 0 了他以为就能用了
  % 足够大比如 100 万就不怕了
  case ets:update_counter(Poolname, Pid, {#conn_info.load, -1000000}) == N - 1000000 of
    true ->
      % 这里先从连接池里摘掉, 尽量避免被使用, 减少 race 的可能性
      ets:delete(Poolname, Pid),
      apply(StopM, StopF, [Pid]),
      true;
    false ->
      % 有 actor 在用, 那这个就不清了, counter改回去, 下个周期再说
      ets:update_counter(Poolname, Pid, {#conn_info.load, 1000000}),
      false
  end.

start_timer() ->
  SweepInterval = get_sweep_interval(),
  erlang:send_after(SweepInterval * 1000, self(), sweep).

get_sweep_interval() ->
  case os:getenv("POOL_SWEEP_INTERVAL") of
    false -> ?SWEEP_INTERVAL;
    Var -> erlang:list_to_integer(Var)
  end.

get_sweep_timeout() ->
  case os:getenv("POOL_CONN_TIMEOUT") of
    false -> ?SWEEP_TIMEOUT;
    Var -> erlang:list_to_integer(Var)
  end.

clean_use_reg(Now) ->
  Expire = Now - ?POOL_REG_EXPIRE_SECONDS,
  MS  = ets:fun2ms(fun(#pool_use_reg{timestamp = Ts} = Reg) when Ts =< Expire -> Reg end),
  NaughtyClients = ets:select(pool_use_reg, MS),
  lists:foreach(fun(#pool_use_reg{
      pid_and_ref = Key, 
      poolname = Poolname, 
      connection_pid = Pid, 
      is_sync = IsSync}) ->
    ets:delete(pool_use_reg, Key),
    Updates = if IsSync -> [{#conn_info.load, -1}, {#conn_info.is_sync, -1, 0, 0}]; 
                 true -> [{#conn_info.load, -1}] 
              end,
    % maybe removed, so we catch here
    catch ets:update_counter(Poolname, Pid, Updates)
  end, NaughtyClients).
