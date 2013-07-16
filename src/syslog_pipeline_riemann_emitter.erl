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
    {start_mfa, {riemann, start_link, []}}
  ]).

send(Events) ->
  Conn = pooler:take_member(?POOL),

  Result = gen_server:call(Conn, {send, [riemann:event(Event) || Event <- Events]}),

  pooler:return_member(?POOL, Conn),

  Result.
