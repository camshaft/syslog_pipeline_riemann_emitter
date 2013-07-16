%%
%% syslog_pipeline_riemann_emitter.erl
%%
-module (syslog_pipeline_riemann_emitter).

-export ([start_link/2]).
-export ([send/1]).

-define (POOL, riemann_pool).

start_link(Host, Port) ->
  application:set_env(riemann, host, Host),
  application:set_env(riemann, port, Port),
  pooler:new_pool([
    {name, ?POOL},
    {max_count, 10},
    {init_count, 2},
    {start_mfa, {gen_server, start_link, [riemann, [], []]}}
  ]).

send(Events) ->
  Conn = pooler:take_member(?POOL),

  REvents = [riemann:event(format_event(Event)) || Event <- Events],

  Result = gen_server:call(Conn, {send, REvents}),

  pooler:return_member(?POOL, Conn),

  Result.

format_event({{_Priority, _Version, DateTime, Hostname, _AppName, _ProcID, _MessageID, _Message}, Parsed}) ->
  Timestamp = format_time(DateTime),
  Service = proplists:get_value(<<"measure">>, Parsed),
  Metric = case proplists:get_value(<<"val">>, Parsed, 0) of
    Val when is_binary(Val) ->
      binary_to_integer(Val);
    Val ->
      Val
  end,
  [
    {time, Timestamp},
    {service, Service},
    {host, Hostname},
    {metric, Metric},
    {ttl, 60}
  ].

format_time(DateTime) ->
  calendar:datetime_to_gregorian_seconds(DateTime) - 62167219200.
