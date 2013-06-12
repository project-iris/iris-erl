%% Iris Erlang  Binding
%% Copyright 2013 Peter Szilagyi. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% decentralized messaging framework, and as such, the same licensing terms
%% hold. For details please see http://github.com/karalabe/iris/LICENSE.md
%%
%% Author: peterke@gmail.com (Peter Szilagyi)

-module(iris).
-export([connect/2, broadcast/3, request/4, reply/2, subscribe/2, publish/3,
	unsubscribe/2, tunnel/3, close/1]).

%% =============================================================================
%% Iris type definitions
%% =============================================================================

-export_type([connection/0, tunnel/0, sender/0]).

-type connection() :: pid().
%% Communication interface to the local Iris node. All messaging within the Iris
%% network must pass through one of these.

-type tunnel() :: pid().
%% Communication stream between the local application and a remote endpoint. The
%% ordered delivery of messages is guaranteed and the message flow between the
%% peers is throttled.

-type sender() :: term().
%% Return address of a request for async replies. It is an arbitrary term within
%% a request message (or handler call in the higher level API).

%% =============================================================================
%% Iris low level API
%% =============================================================================

%% @doc Connects to the iris message relay running locally, registering with the
%%      specified app name. The calling process will receive all inbound events.
%%
%% @spec (Port, App) -> {ok, Connection} | {error, Reason}
%%      Port       = pos_integer()
%%      App        = string()
%%      Connection = connection()
%%      Reason     = atom()
%% @end
-spec connect(Port :: pos_integer(), App :: string()) ->
	{ok, Connection :: connection()} | {error, Reason :: atom()}.

connect(Port, App) ->
	iris_relay:connect(Port, App).

%% @doc Broadcasts a message to all applications of type app. No guarantees are
%%      made that all recipients receive the message (best effort).
%%
%%      The call blocks until the message is sent to the relay node.
%%
%% @spec (Connection, App, Message) -> ok | {error, Reason}
%%      Connection = connection()
%%      App        = string()
%%      Message    = binary()
%%      Reason     = atom()
%% @end
-spec broadcast(Connection :: connection(), App :: string(), Message :: binary()) ->
	ok | {error, Reason :: atom()}.

broadcast(Connection, App, Message) ->
	iris_relay:broadcast(Connection, App, Message).

%% @doc Executes a synchronous request to app, load balanced between all the
%%      active ones, returning the received reply.
%%
%%      The call blocks until either a reply arrives or the request times out.
%%
%% @spec (Connection, App, Request, Timeout) -> {ok, Reply} | {error, Reason}
%%      Connection = connection()
%%      App        = string()
%%      Request    = binary()
%%      Timeout    = pos_integer()
%%      Reply      = binary()
%%      Reason     = timeout | atom()
%% @end
-spec request(Connection :: connection(), App :: string(), Request :: binary(), Timeout :: pos_integer()) ->
	{ok, Reply :: binary()} | {error, Reason :: atom()}.

request(Connection, App, Request, Timeout) ->
	iris_relay:request(Connection, App, Request, Timeout).

%% @doc Remote pair of the request function. Should be used to send back a reply
%%      to the request origin.
%%
%%      The call blocks until the message is sent to the relay node.
%%
%%      Sender must be the Sender argument from the request message.
%%
%% @spec (Sender, Reply) -> ok | {error, Reason}
%%      Sender = sender()
%%      Reply  = binary()
%%      Reason = atom()
%% @end
-spec reply(Sender :: iris:sender(), Reply :: binary()) ->
	ok | {error, Reason :: atom()}.

reply(Sender, Reply) ->
	iris_relay:reply(Sender, Reply).

%% @doc Subscribes to a topic, receiving events as process messages.
%%
%%      The call blocks until the message is sent to the relay node.
%%
%% @spec (Connection, Topic) -> ok | {error, Reason}
%%      Connection = connection()
%%      Topic      = string()
%%      Reason     = atom()
%% @end
-spec subscribe(Connection :: connection(), Topic :: string()) ->
	ok | {error, Reason :: atom()}.

subscribe(Connection, Topic) ->
	iris_relay:subscribe(Connection, Topic).

%% @doc Publishes an event to all applications subscribed to the topic. No
%%      guarantees are made that all subscribers receive the message (best
%%      effort).
%%
%%      The call blocks until the message is sent to the relay node.
%%
%% @spec (Connection, Topic, Event) -> ok | {error, Reason}
%%      Connection = connection()
%%      Topic      = string()
%%      Event      = binary()
%%      Reason     = atom()
%% @end
-spec publish(Connection :: connection(), Topic :: string(), Event :: binary()) ->
	ok | {error, Reason :: atom()}.

publish(Connection, Topic, Event) ->
	iris_relay:publish(Connection, Topic, Event).

%% @doc Unsubscribes from a previously subscribed topic.
%%
%%      The call blocks until the message is sent to the relay node.
%%
%% @spec (Connection, Topic) -> ok | {error, Reason}
%%      Connection = connection()
%%      Topic      = string()
%%      Reason     = atom()
%% @end
-spec unsubscribe(Connection :: connection(), Topic :: string()) ->
	ok | {error, Reason :: atom()}.

unsubscribe(Connection, Topic) ->
	iris_relay:unsubscribe(Connection, Topic).

%% @doc Opens a direct tunnel to an instance of app, allowing pairwise-exclusive
%%      and order-guaranteed message passing between them.
%%
%%      The call blocks until the either the newly created tunnel is set up, or
%%      a timeout occurs.
%%
%% @spec (Connection, App, Timeout) -> {ok, Tunnel} | {error, Reason}
%%      Connection = connection()
%%      App        = string()
%%      Timeout    = pos_integer()
%%      Tunnel     = tunnel()
%%      Reason     = timeout | atom()
%% @end
-spec tunnel(Connection :: connection(), App :: string(), Timeout :: pos_integer()) ->
	{ok, Tunnel :: tunnel()} | {error, Reason :: atom()}.

tunnel(Connection, App, Timeout) ->
	iris_relay:tunnel(Connection, App, Timeout).

%% @doc Gracefully terminates the connection removing all subscriptions and
%%      closing all tunnels.
%%
%%      The call blocks until the connection is torn down or an error occurs.
%%
%% @spec (Connection) -> ok | {error, Reason}
%%      Connection = connection()
%%      Reason     = atom()
%% @end
-spec close(Connection :: connection()) ->
	ok | {error, Reason :: atom()}.

close(Connection) ->
  iris_relay:close(Connection).
