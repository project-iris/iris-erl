%% Iris Erlang binding
%% Copyright (c) 2014 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

%% @doc Module responsible for initiating communication within the Iris network.
%% @end

-module(iris_client).
-export([start/1, start_link/1, stop/1]).
-export([broadcast/3, request/4, subscribe/4, subscribe/5, publish/3,
	unsubscribe/2, tunnel/3, logger/1]).
-export_type([client/0]).


-type client() :: pid(). %% Client interface to the local Iris node. All outbound
                         %% messaging passes through one of these.


%% @doc Connects to the Iris network as a simple client.
%%
%% @spec (Port) -> {ok, Client} | {error, Reason}
%%      Port   = pos_integer()
%%      Client = iris_client:client()
%%      Reason = term()
%% @end
-spec start(Port :: pos_integer()) ->	{ok, Client :: client()} | {error, Reason :: term()}.

start(Port) ->
  Logger = iris_logger:new([{client, iris_counter:next_id(client)}]),
  iris_logger:info(Logger, "connecting new client", [{relay_port, Port}]),

  Result = iris_conn:connect(Port, Logger),
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
%%      Client = iris_client:client()
%%      Reason = term()
%% @end
-spec start_link(Port :: pos_integer()) -> {ok, Client :: client()} | {error, Reason :: term()}.

start_link(Port) ->
  Logger = iris_logger:new([{client, iris_counter:next_id(client)}]),
  iris_logger:info(Logger, "connecting and linking new client", [{relay_port, Port}]),

  Result = iris_conn:connect_link(Port, Logger),
  case Result of
    {ok, _Pid}      -> iris_logger:info(Logger, "client connection established");
    {error, Reason} -> iris_logger:warn(Logger, "failed to connect new client", [{reason, Reason}])
  end,
  Result.


%% @doc Gracefully terminates the connection removing all subscriptions and
%%      closing all active tunnels.
%%
%%      The call blocks until the connection tear-down is confirmed by the Iris
%%      node.
%%
%% @spec (Client) -> ok | {error, Reason}
%%      Client = iris_client:client()
%%      Reason = term()
%% @end
-spec stop(Client :: client()) -> ok | {error, Reason :: term()}.

stop(Client) ->	iris_conn:close(Client).


%% @doc Broadcasts a message to all members of a cluster. No guarantees are made
%%      that all recipients receive the message (best effort).
%%
%%      The call blocks until the message is forwarded to the local Iris node.
%%
%% @spec (Client, Cluster, Message) -> ok
%%      Client  = iris_client:client()
%%      Cluster = string()
%%      Message = binary()
%% @end
-spec broadcast(Client :: client(), Cluster :: string(), Message :: binary()) -> ok.

broadcast(Client, Cluster, Message) ->
	ok = iris_conn:broadcast(Client, Cluster, Message).


%% @doc Executes a synchronous request to be serviced by a member of the
%%      specified cluster, load-balanced between all participant, returning
%%      the received reply.
%%
%%      The timeout unit is in milliseconds. Infinity is not supported!
%%
%% @spec (Client, Cluster, Request, Timeout) -> {ok, Reply} | {error, Reason}
%%      Client  = iris_client:client()
%%      Cluster = string()
%%      Request = binary()
%%      Timeout = pos_integer()
%%      Reply   = binary()
%%      Reason  = timeout | string()
%% @end
-spec request(Client :: client(), Cluster :: string(), Request :: binary(), Timeout :: pos_integer()) ->
  {ok, Reply :: binary()} | {error, Reason :: (timeout | string())}.

request(Client, Cluster, Request, Timeout) ->
  iris_conn:request(Client, Cluster, Request, Timeout).


%% @doc Subscribes to a topic, using an iris_topic behavior as the callback for
%%      arriving events.
%%
%%      The method blocks until the subscription is forwarded to the relay.
%%      There might be a small delay between subscription completion and start of
%%      event delivery. This is caused by subscription propagation through the
%%      network.
%%
%% @spec (Client, Topic, Module, Args) -> ok | {error, Reason}
%%      Client = iris_client:client()
%%      Topic  = string()
%%      Module = atom()
%%      Args   = term()
%%      Reason = atom()
%% @end
-spec subscribe(Client :: client(), Topic :: string(), Module :: atom(),	Args :: term()) ->
	ok | {error, Reason :: atom()}.

subscribe(Client, Topic, Module, Args) ->
	subscribe(Client, Topic, Module, Args, []).


%% @doc Subscribes to a topic, using an iris_topic behavior as the callback for
%%      arriving events.
%%
%%      The method blocks until the subscription is forwarded to the relay.
%%      There might be a small delay between subscription completion and start of
%%      event delivery. This is caused by subscription propagation through the
%%      network.
%%
%% @spec (Client, Topic, Module, Args, Options) -> ok | {error, Reason}
%%      Client  = iris_client:client()
%%      Topic   = string()
%%      Module  = atom()
%%      Args    = term()
%%      Options = [Option]
%%        Option = {event_memory, Limit}
%%          Limit = pos_integer()
%%      Reason  = atom()
%% @end
-spec subscribe(Client :: client(), Topic :: string(), Module :: atom(),	Args :: term(),
	Options :: [{atom(), term()}]) ->	ok | {error, Reason :: atom()}.

subscribe(Client, Topic, Module, Args, Options) ->
	iris_conn:subscribe(Client, Topic, Module, Args, Options).


%% @doc Publishes an event asynchronously to topic. No guarantees are made that
%%      all subscribers receive the message (best effort).
%%
%%      The method blocks until the message is forwarded to the local Iris node.
%%
%% @spec (Client, Topic, Event) -> ok | {error, Reason}
%%      Client = iris_client:client()
%%      Topic  = string()
%%      Event  = binary()
%%      Reason = atom()
%% @end
-spec publish(Client :: client(), Topic :: string(), Event :: binary()) ->
	ok | {error, Reason :: atom()}.

publish(Client, Topic, Event) ->
	iris_conn:publish(Client, Topic, Event).


%% @doc Unsubscribes from topic, receiving no more event notifications for it.
%%
%%      The method blocks until the unsubscription is forwarded to the local Iris node.
%%
%% @spec (Client, Topic) -> ok | {error, Reason}
%%      Client = iris_client:client()
%%      Topic  = string()
%%      Reason = atom()
%% @end
-spec unsubscribe(Client :: client(), Topic :: string()) ->
	ok | {error, Reason :: atom()}.

unsubscribe(Client, Topic) ->
	iris_conn:unsubscribe(Client, Topic).


%% @doc Opens a direct tunnel to a member of a remote cluster, allowing pairwise-exclusive,
%%      order-guaranteed and throttled message passing between them.
%%
%%      The method blocks until the newly created tunnel is set up, or the time
%%      limit is reached.
%%
%% @spec (Client, Cluster, Timeout) -> {ok, Tunnel} | {error, Reason}
%%      Client  = iris_client:client()
%%      Cluster = string()
%%      Timeout = pos_integer()
%%      Tunnel  = iris_tunnel:tunnel()
%%      Reason  = timeout | atom()
%% @end
-spec tunnel(Client :: client(), Cluster :: string(), Timeout :: pos_integer()) ->
	{ok, Tunnel :: iris_tunnel:tunnel()} | {error, Reason :: atom()}.

tunnel(Client, Cluster, Timeout) ->
	iris_conn:tunnel(Client, Cluster, Timeout).


%% @doc Retrieves the contextual logger associated with the client.
%%
%% @spec (Client) -> Logger
%%      Client = iris_client:client()
%%      Logger = iris_logger:logger()
%% @end
logger(Client) ->
	iris_conn:logger(Client).
