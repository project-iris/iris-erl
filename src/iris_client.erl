%% Iris Erlang binding
%% Copyright (c) 2014 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

-module(iris_client).
-export([start/1, start_link/1, stop/1]).
-export([broadcast/3, request/4, subscribe/5, publish/3, unsubscribe/2, tunnel/3]).


%% @doc Connects to the Iris network as a simple client.
%%
%% @spec (Port) -> {ok, Client} | {error, Reason}
%%      Port   = pos_integer()
%%      Client = pid()
%%      Reason = term()
%% @end
-spec start(Port :: pos_integer()) ->	{ok, Client :: pid()} | {error, Reason :: term()}.

start(Port) ->
  Logger = iris_logger:new([{client, iris_counter:next_id(client)}]),
  iris_logger:info(Logger, "connecting new client", [{relay_port, Port}]),

  Result = iris_conn:connect(Port),
  case Result of
    {ok, _Pid}      -> iris_logger:info(Logger, "client connection established");
    {error, Reason} -> iris_logger:warn(Logger, "failed to connect new client", [{reason, Reason}])
  end,
  Result.


%% @doc Connects to the Iris network as a simple client and links the spawned
%%      process to the current one.
%%
%% @spec (Port) -> {ok, Client} | {error, Reason}
%%      Port   = pos_integer()
%%      Client = pid()
%%      Reason = term()
%% @end
-spec start_link(Port :: pos_integer()) -> {ok, Client :: pid()} | {error, Reason :: term()}.

start_link(Port) ->
  Logger = iris_logger:new([{client, iris_counter:next_id(client)}]),
  iris_logger:info(Logger, "connecting and linking new client", [{relay_port, Port}]),

  Result = iris_conn:connect_link(Port),
  case Result of
    {ok, _Pid}      -> iris_logger:info(Logger, "client connection established");
    {error, Reason} -> iris_logger:warn(Logger, "failed to connect new client", [{reason, Reason}])
  end,
  Result.


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
  iris_conn:request(Client, Cluster, Request, Timeout).


%% @doc Subscribes to a topic, using handler as the callback for arriving events.
%%
%%      The method blocks until the subscription is forwarded to the relay.
%%      There might be a small delay between subscription completion and start of
%%      event delivery. This is caused by subscription propagation through the
%%      network.
%%
%% @spec (Client, Topic) -> ok | {error, Reason}
%%      Client = pid()
%%      Topic      = string()
%%      Reason     = atom()
%% @end
-spec subscribe(Client :: pid(), Topic :: string(), Module :: atom(),	Args :: term(),
	Options :: [{atom(), term()}]) ->	ok | {error, Reason :: atom()}.

subscribe(Client, Topic, Module, Args, Options) ->
	iris_conn:subscribe(Client, Topic, Module, Args, Options).


%% @doc Publishes an event to all applications subscribed to the topic. No
%%      guarantees are made that all subscribers receive the message (best
%%      effort).
%%
%%      The call blocks until the message is sent to the relay node.
%%
%% @spec (Client, Topic, Event) -> ok | {error, Reason}
%%      Client = pid()
%%      Topic      = string()
%%      Event      = binary()
%%      Reason     = atom()
%% @end
-spec publish(Client :: pid(), Topic :: string(), Event :: binary()) ->
	ok | {error, Reason :: atom()}.

publish(Client, Topic, Event) ->
	iris_conn:publish(Client, Topic, Event).


%% @doc Unsubscribes from a previously subscribed topic.
%%
%%      The call blocks until the message is sent to the relay node.
%%
%% @spec (Client, Topic) -> ok | {error, Reason}
%%      Client = pid()
%%      Topic      = string()
%%      Reason     = atom()
%% @end
-spec unsubscribe(Client :: pid(), Topic :: string()) ->
	ok | {error, Reason :: atom()}.

unsubscribe(Client, Topic) ->
	iris_client:unsubscribe(Client, Topic).


%% @doc Opens a direct tunnel to an instance of app, allowing pairwise-exclusive
%%      and order-guaranteed message passing between them.
%%
%%      The call blocks until the either the newly created tunnel is set up, or
%%      a timeout occurs.
%%
%% @spec (Client, Cluster, Timeout) -> {ok, Tunnel} | {error, Reason}
%%      Client = pid()
%%      Cluster        = string()
%%      Timeout    = pos_integer()
%%      Tunnel     = tunnel()
%%      Reason     = timeout | atom()
%% @end
-spec tunnel(Client :: pid(), Cluster :: string(), Timeout :: pos_integer()) ->
	{ok, Tunnel :: pid()} | {error, Reason :: atom()}.

tunnel(Client, Cluster, Timeout) ->
	iris_conn:tunnel(Client, Cluster, Timeout).
