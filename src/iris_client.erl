%% Iris Erlang binding
%% Copyright (c) 2014 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

-module(iris_client).
-export([start/1, start_link/1, stop/1]).
-export([broadcast/3, request/4]).


-spec start(Port :: pos_integer()) ->
	{ok, Client :: pid()} | {error, Reason :: term()}.

start(Port) -> iris_conn:connect(Port).


-spec start_link(Port :: pos_integer()) ->
	{ok, Client :: pid()} | {error, Reason :: term()}.

start_link(Port) ->	iris_conn:connect_link(Port).


-spec stop(Client :: pid()) ->
	ok | {error, Reason :: term()}.

stop(Client) ->	iris_conn:close(Client).


%% @doc Broadcasts a message to all members of a cluster. No guarantees are made
%%      that all recipients receive the message (best effort).
%%
%%      The call blocks until the message is forwarded to the local Iris node.
%%
%% @spec (Client, Cluster, Message) -> ok
%%      Client  = pid()
%%      Cluster = string()
%%      Message = binary()
%% @end
-spec broadcast(Client :: pid(), Cluster :: string(), Message :: binary()) -> ok.

broadcast(Client, Cluster, Message) ->
	ok = iris_conn:broadcast(Client, Cluster, Message).


%% @doc Executes a synchronous request to be serviced by a member of the
%%      specified cluster, load-balanced between all participant, returning
%%      the received reply.
%%
%%      The timeout unit is in milliseconds. Infinity is not supported!
%%
%% @spec (Client, Cluster, Request, Timeout) -> {ok, Reply} | {error, Reason}
%%      Client  = pid()
%%      Cluster = string()
%%      Request = binary()
%%      Timeout = pos_integer()
%%      Reply   = binary()
%%      Reason  = timeout | string()
%% @end
-spec request(Client :: pid(), Cluster :: string(), Request :: binary(), Timeout :: pos_integer()) ->
  {ok, Reply :: binary()} | {error, Reason :: (timeout | string())}.

request(Client, Cluster, Request, Timeout) ->
  iris_relay:request(Client, Cluster, Request, Timeout).
