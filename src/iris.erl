%% Iris Erlang Binding
%% Copyright 2013 Peter Szilagyi. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% decentralized messaging framework, and as such, the same licensing terms
%% hold. For details please see http://github.com/karalabe/iris/LICENSE.md
%%
%% Author: peterke@gmail.com (Peter Szilagyi)

-module(iris).
-export([reload/0]).
-export([connect/3, version/0, close/1]).
-export([broadcast/3]).

%% @doc Returns the relay protocol version implemented. Connecting to an Iris
%%      node will fail unless the versions match exactly.
%%
%% @spec () -> Version
%%      Version = string()
%% @end
version() -> iris_proto:version().

%% @doc Connects to the iris message relay running locally, registering with the
%%      specified app name and setting the inbound event handler.
%%
%% @spec (Port, App, Handler) -> {ok, Connection} | {error, Reason}
%%      Port       = integer()
%%      App        = string()
%%      Handler    = pid()
%%      Connection = pid()
%%      Reason     = term()
%% @end
connect(Port, App, Handler) ->
	iris_relay:connect(Port, App, Handler).

broadcast(Connection, App, Message) ->
  ok.

%% @doc Gracefully terminates the connection removing all subscriptions and
%%      closing all tunnels.
%%
%%      The method blocks until the connection is torn down or an error occurs.
%%
%% @spec (Connection) -> ok | {error, Reason}
%%      Connection = pid()
%%      Reason     = term()
%% @end
close(Connection) ->
  iris_relay:close(Connection).

%% =============================================================================
%% Ugly dev hacks follow, nothing to see here :))
%% =============================================================================

%% Reloads all iris related modules. Just a dev hack to make my life easier.
reload() ->
	Modules = [M || {M, P} <- code:all_loaded(), is_list(P) andalso string:str(P, "/work/iris") > 0],
	lists:foreach(fun reload/1, Modules).

%% Reloads a specific module. Just a dev hack to make my life easier.
reload(Module) ->
  code:purge(Module),
  code:soft_purge(Module),
  {module, Module} = code:load_file(Module),
  ok.

