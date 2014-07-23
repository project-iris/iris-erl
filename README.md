  Iris Erlang binding
=======================

This is the official Erlang language binding for the Iris cloud messaging framework. Version `v1` of the binding is compatible with Iris `v0.3.0` and newer.

If you are unfamiliar with Iris, please read the next introductory section. It contains a short summary, as well as some valuable pointers on where you can discover more.

  Background
-------------------

Iris is an attempt at bringing the simplicity and elegance of cloud computing to the application layer. Consumer clouds provide unlimited virtual machines at the click of a button, but leaves it to developer to wire them together. Iris ensures that you can forget about networking challenges and instead focus on solving your own domain problems.

It is a completely decentralized messaging solution for simplifying the design and implementation of cloud services. Among others, Iris features zero-configuration (i.e. start it up and it will do its magic), semantic addressing (i.e. application use textual names to address each other), clusters as units (i.e. automatic load balancing between apps of the same name) and perfect secrecy (i.e. all network traffic is encrypted).

You can find further infos on the [Iris website](http://iris.karalabe.com) and details of the above features in the [core concepts](http://iris.karalabe.com/book/core_concepts) section of [the book of Iris](http://iris.karalabe.com/book). For the scientifically inclined, a small collection of [papers](http://iris.karalabe.com/papers) is also available featuring Iris. Slides and videos of previously given public presentations are published in the [talks](http://iris.karalabe.com/talks) page.

There is a growing community on Twitter [@iriscmf](https://twitter.com/iriscmf), Google groups [project-iris](https://groups.google.com/group/project-iris) and GitHub [project-iris](https://github.com/project-iris).

  Installation
----------------

To import this package, add the following line to `deps` in `rebar.config`:

    {iris, ".*", {git, "https://github.com/project-iris/iris-erl.git", {branch, "v1"}}}

To retrieve the package, execute:

    rebar get-deps

Refer to it as _iris_.

  Quickstart
--------------

Iris uses a relaying architecture, where client applications do not communicate directly with one another, but instead delegate all messaging operations to a local relay process responsible for transferring the messages to the correct destinations. The first step hence to using Iris through any binding is setting up the local [_relay_ _node_](http://iris.karalabe.com/downloads). You can find detailed infos in the [Run, Forrest, Run](http://iris.karalabe.com/book/run_forrest_run) section of [the book of Iris](http://iris.karalabe.com/book), but a very simple way would be to start a _developer_ node.

    > iris -dev
    Entering developer mode
    Generating random RSA key... done.
    Generating random network name... done.

    2014/06/13 18:13:47 main: booting iris overlay...
    2014/06/13 18:13:47 scribe: booting with id 369650985814.
    2014/06/13 18:13:57 main: iris overlay converged with 0 remote connections.
    2014/06/13 18:13:57 main: booting relay service...
    2014/06/13 18:13:57 main: iris successfully booted, listening on port 55555.

Since it generates random credentials, a developer node will not be able to connect with other remote nodes in the network. However, it provides a quick solution to start developing without needing to configure a _network_ _name_ and associated _access_ _key_. Should you wish to interconnect multiple nodes, please provide the `-net` and `-rsa` flags.

### Attaching to the relay

After successfully booting, the relay opens a _local_ TCP endpoint (port `55555` by default, configurable using `-port`) through which arbitrarily many entities may attach. Each connecting entity may also decide whether it becomes a simple _client_ only consuming the services provided by other participants, or a full fledged _service_, also making functionality available to others for consumption.

Connecting as a client can be done trivially by `iris_client:start/1` or `iris_client:start_link/1` with the port number of the local relay's client endpoint. After the attachment is completed, a connection `pid` is returned through which messaging can begin. A client cannot accept inbound requests, broadcasts and tunnels, only initiate them.

```erlang
% Connect to the local relay
{ok, Conn} = iris_client:start(55555),

% Disconnect from the local relay
ok = iris_client:stop(Conn)
```

To provide functionality for consumption, an entity needs to register as a service. This is slightly more involved, as beside initiating a registration request, it also needs to specify a callback handler to process inbound events. First, the callback handler needs to implement the `iris_server` behavior. After writing the handler, registration can commence by invoking `iris_server:start/4,/5` or `iris_server:start_link/4,/5` with the port number of the local relay's client endpoint; sub-service cluster this entity will join as a member; callback module to process inbound messages; initialization arguments for the callback module and optional resource caps.

```erlang
-behaviour(iris_server).
-export([init/2, handle_broadcast/2, handle_request/3, handle_tunnel/2,
	handle_drop/2, terminate/2]).

% Implement all the methods defined by iris_server.
init(Conn, your_init_args) -> {ok, your_state}.
terminate(Reason, State)   -> ok.

handle_broadcast(Message, State)     -> {noreply, State}.
handle_request(Request, From, State) -> {reply, Request, State}.
handle_tunnel(Tunnel, State)         -> {noreply, State}.
handle_drop(Reason, State)           -> {stop, Reason, State}.

main() ->
	% Register a new service to the relay
	{ok, Server} = iris_server:start(55555, "echo", ?MODULE, your_init_args),

	% Unregister the service
	ok = iris_server:stop(Server).
```

Upon successful registration, Iris invokes the callback module's `init/2` method with the live connection `pid` - the service's client connection - through which the service itself can initiate outbound requests, and the user supplied initialization arguments. The `init/2` is called only once and before any other handler method.

### Messaging through Iris

Iris supports four messaging schemes: request/reply, broadcast, tunnel and publish/subscribe. The first three schemes always target a specific cluster: send a request to _one_ member of a cluster and wait for the reply; broadcast a message to _all_ members of a cluster; open a streamed, ordered and throttled communication tunnel to _one_ member of a cluster. The publish/subscribe is similar to broadcast, but _any_ member of the network may subscribe to the same topic, hence breaking cluster boundaries.

<img src="https://dl.dropboxusercontent.com/u/10435909/Iris/messaging_schemes.png" style="height: 175px; display: block; margin-left: auto; margin-right: auto;" \>

Presenting each primitive is out of scope, but for illustrative purposes the request/reply was included. Given the echo service registered above, we can send it requests and wait for replies through any client connection. Iris will automatically locate, route and load balanced between all services registered under the addressed name.

```erlang
Request = <<"some request binary">>,
case iris_client:request(Conn, "echo", Request, 1000) of
	{ok, Reply}     -> io:format("Reply arrived: ~p.~n", [Reply]);
	{error, Reason} -> io:format("Failed to execute request: ~p.~n", [Reason])
end
```

An expanded summary of the supported messaging schemes can be found in the [core concepts](http://iris.karalabe.com/book/core_concepts) section of [the book of Iris](http://iris.karalabe.com/book). A detailed presentation and analysis of each individual primitive will be added soon.

### Error handling

The binding uses the idiomatic Erlang error handling mechanisms of returning `{error, Reason}` tuples when a failure occurs. However, there are a few common cases that need to be individually checkable, hence a few special errors values and types have been introduced.

Many operations - such as requests and tunnels - can time out. To allow checking for this particular failure, Iris returns `{error, timeout}` in such scenarios. Similarly, connections, services and tunnels may fail, in the case of which all pending operations terminate with `{error, closed}`.

Additionally, the requests/reply pattern supports sending back an error instead of a reply to the caller. To enable the originating node to check whether a request failed locally or remotely, all remote error reasons are wrapped in an `{remote, Reason}` tuple.

```erlang
case iris_client:request(Conn, "cluster", Request, Timeout) of
	{ok, Reply} ->
		% Request completed successfully
	{error, timeout} ->
		% Request timed out
	{error, closed} ->
		% Connection terminated
	{error, {remote, Reason}} ->
		% Request failed remotely
	{error, Reason} ->
		% Requesting failed locally
end
```

### Resource capping

To prevent the network from overwhelming an attached process, the binding places memory limits on the broadcasts/requests inbound to a registered service as well as on the events received by a topic subscription. The memory limit defines the maximal length of the pending queue.

The default values - listed below - can be overridden during service registration via `{broadcast_memory, Limit}`, `{request_memory, Limit}` and during topic subscription via `{event_memory, Limit}` passed as options. Any unset options will default to the preset ones.

```erlang
%% Memory allowance for pending broadcasts.
default_broadcast_memory() -> 64 * 1024 * 1024.

%% Memory allowance for pending requests.
default_request_memory() -> 64 * 1024 * 1024.

%% Memory allowance for pending events.
default_topic_memory() -> 64 * 1024 * 1024.
```

There is also a sanity limit on the input buffer of a tunnel, but it is not exposed through the API as tunnels are meant as structural primitives, not sensitive to load. This may change in the future.

### Logging

For logging purposes, the Erlang binding uses [basho](https://github.com/basho)'s [lager](https://github.com/basho/lager) library (version 2.0.3). By default, _INFO_ level logs are collected and printed to _stdout_. This level allows tracking life-cycle events such as client and service attachments, topic subscriptions and tunnel establishments. Further log entries can be requested by lowering the level to _DEBUG_, effectively printing all messages passing through the binding.

The logger can be fine-tuned through the `iris_logger` module. Below are a few common configurations.

```erlang
% Discard all log entries
iris_logger:level(none),

// Log DEBUG level entries
iris_logger:level(debug),
```

Note, that log levels permitted by the binding may still be filtered out by _lager_ and vice versa. This is an intentional feature to allow silencing the binding even when _lager_ would allow more detailed logs.

To be finished...

### Additional goodies

You can find a teaser presentation, touching on all the key features of the library through a handful of challenges and their solutions. The recommended version is the [playground](http://play.iris.karalabe.com/talks/binds/erlang.slide), containing modifiable and executable code snippets, but a [read only](http://iris.karalabe.com/talks/binds/erlang.slide) one is also available.

  Contributions
-----------------

Currently my development aims are to stabilize the project and its language bindings. Hence, although I'm open and very happy for any and all contributions, the most valuable ones are tests, benchmarks and actual binding usage to reach a high enough quality.

Due to the already significant complexity of the project (Iris in general), I kindly ask anyone willing to pinch in to first file an [issue](https://github.com/project-iris/iris-erl/issues) with their plans to achieve a best possible integration :).

Additionally, to prevent copyright disputes and such, a signed contributor license agreement is required to be on file before any material can be accepted into the official repositories. These can be filled online via either the [Individual Contributor License Agreement](http://iris.karalabe.com/icla) or the [Corporate Contributor License Agreement](http://iris.karalabe.com/ccla).
