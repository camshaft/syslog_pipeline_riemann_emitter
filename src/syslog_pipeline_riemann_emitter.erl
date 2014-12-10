%%
%% syslog_pipeline_riemann_emitter.erl
%%
-module (syslog_pipeline_riemann_emitter).

-export ([start_link/2]).
-export ([send/1]).

-define (POOL, riemann_pool).

start_link(Host, Port) ->
  application:start(pooler),
  application:set_env(riemann, host, Host),
  application:set_env(riemann, port, Port),
  pooler:new_pool([
    {name, ?POOL},
    {max_count, 10},
    {init_count, 2},
    {start_mfa, {gen_server, start_link, [riemann, [], []]}}
  ]).

send([]) ->
  ok;
send(Events) ->
  Conn = pooler:take_member(?POOL),

  REvents = [riemann:event(format_event(Event)) || Event <- Events],

  Result = gen_server:call(Conn, {send, REvents}),

  pooler:return_member(?POOL, Conn),

  Result.

format_event({{_Priority, _Version, DateTime, Hostname, AppName, _ProcID, _MessageID, _Message}, Parsed}) ->
  Timestamp = format_time(DateTime),
  Service = proplists:get_value(<<"measure">>, Parsed),
  Val = proplists:get_value(<<"val">>, Parsed, <<"0">>),
  Tags = proplists:get_value(<<"tags">>, Parsed, []),
  Metric = binary_to_number(Val),
  [
    {time, Timestamp},
    {service, Service},
    {host, <<Hostname/binary, ".", AppName/binary>>},
    {metric, Metric},
    {ttl, 360},
    {tags, [Hostname|Tags]}
  ].

format_time(DateTime) ->
  calendar:datetime_to_gregorian_seconds(DateTime) - 62167219200.

binary_to_number(Bin)->
  case catch binary_to_float(Bin) of
    N when is_float(N) -> N;
    _ ->
      case catch binary_to_integer(Bin) of
        N when is_integer(N) -> N;
        _ -> 0
      end
  end.
