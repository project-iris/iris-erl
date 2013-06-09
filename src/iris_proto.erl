%% Iris Erlang Binding
%% Copyright 2013 Peter Szilagyi. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% decentralized messaging framework, and as such, the same licensing terms
%% hold. For details please see http://github.com/karalabe/iris/LICENSE.md
%%
%% Author: peterke@gmail.com (Peter Szilagyi)

-module(iris_proto).

-export([version/0]).
-export([sendInit/2, sendBroadcast/3, sendRequest/5, sendReply/3,
	sendSubscribe/2, sendPublish/3, sendUnsubscribe/2, sendClose/1,
	sendTunnelRequest/5, sendTunnelReply/4, sendTunnelData/3, sendTunnelAck/2,
	sendTunnelClose/2, process/3, procInit/1]).

-define(OP_INIT,      16#00). %% Connection initialization
-define(OP_BCAST,     16#01). %% Application broadcast
-define(OP_REQ,       16#02). %% Application request
-define(OP_REP,       16#03). %% Application reply
-define(OP_SUB,       16#04). %% Topic subscription
-define(OP_PUB,       16#05). %% Topic publish
-define(OP_UNSUB,     16#06). %% Topic subscription removal
-define(OP_CLOSE,     16#07). %% Connection closing
-define(OP_TUN_REQ,   16#08). %% Tunnel building request
-define(OP_TUN_REP,   16#09). %% Tunnel building reply
-define(OP_TUN_DATA,  16#0a). %% Tunnel data transfer
-define(OP_TUN_ACK,   16#0b). %% Tunnel data acknowledgement
-define(OP_TUN_CLOSE, 16#0c). %% Tunnel closing

%% Implemented relay protocol version.
version() -> "v1.0".

%% Serializes a single byte into the relay.
sendByte(Socket, Byte) ->
	gen_tcp:send(Socket, [Byte]).

%% Serializes a variable int into the relay.
sendVarint(Socket, Num) when Num < 128 -> sendByte(Socket, Num);
sendVarint(Socket, Num) ->
	Rem = Num rem 128,
	Res = Num div 128,
	case sendByte(Socket, 128 + Rem) of
		ok    -> sendVarint(Socket, Res);
		Error -> Error
	end.

%% Serializes a length-tagged binary array into the relay.
sendBinary(Socket, Binary) ->
	case sendVarint(Socket, byte_size(Binary)) of
		ok    -> gen_tcp:send(Socket, Binary);
		Error -> Error
	end.

%% Serializes a length-tagged string into the relay.
sendString(Socket, String) ->
	sendBinary(Socket, list_to_binary(String)).

%% Assembles and serializes an init packet into the relay.
sendInit(Socket, App) ->
	case sendByte(Socket, ?OP_INIT) of
		ok ->
			case sendString(Socket, version()) of
				ok    -> sendString(Socket, App);
				Error -> Error
			end;
		Error -> Error
	end.

%% Assembles and serializes a broadcast packet into the relay.
sendBroadcast(Socket, App, Message) ->
	case sendByte(Socket, ?OP_BCAST) of
		ok ->
			case sendString(Socket, App) of
				ok    -> sendBinary(Socket, Message);
				Error -> Error
			end;
		Error -> Error
	end.

%% Assembles and serializes a request packet into the relay.
sendRequest(Socket, ReqId, App, Req, Timeout) ->
	case sendByte(Socket, ?OP_REQ) of
		ok ->
			case sendVarint(Socket, ReqId) of
				ok ->
					case sendString(Socket, App) of
						ok ->
							case sendBinary(Socket, Req) of
								ok    -> sendVarint(Socket, Timeout);
								Error -> Error
							end;
						Error -> Error
					end;
				Error -> Error
			end;
		Error -> Error
	end.

%% Assembles and serializes a reply packet into the relay.
sendReply(Socket, ReqId, Reply) ->
	case sendByte(Socket, ?OP_REP) of
		ok ->
			case sendVarint(Socket, ReqId) of
				ok    -> sendBinary(Socket, Reply);
				Error -> Error
			end;
		Error -> Error
	end.

%% Assembles and serializes a subscription packet into the relay.
sendSubscribe(Socket, Topic) ->
	case sendByte(Socket, ?OP_SUB) of
		ok    -> sendString(Socket, Topic);
		Error -> Error
	end.

%% Assembles and serializes a message to be published in a topic.
sendPublish(Socket, Topic, Message) ->
	case sendByte(Socket, ?OP_PUB) of
		ok ->
			case sendString(Socket, Topic) of
				ok    -> sendBinary(Socket, Message);
				Error -> Error
			end;
		Error -> Error
	end.

%% Assembles and serializes a subscription removal packet into the relay.
sendUnsubscribe(Socket, Topic) ->
	case sendByte(Socket, ?OP_UNSUB) of
		ok    -> sendString(Socket, Topic);
		Error -> Error
	end.

%% Assembles and serializes a close packet into the relay.
sendClose(Socket) ->
	sendByte(Socket, ?OP_CLOSE).

%% Assembles and serializes a tunneling packet into the relay.
sendTunnelRequest(Socket, TunId, App, Buffer, Timeout) ->
	case sendByte(Socket, ?OP_TUN_REQ) of
		ok ->
			case sendVarint(Socket, TunId) of
				ok ->
					case sendString(Socket, App) of
						ok ->
							case sendVarint(Socket, Buffer) of
								ok    -> sendVarint(Socket, Timeout);
								Error -> Error
							end;
						Error -> Error
					end;
				Error -> Error
			end;
		Error -> Error
	end.

%% Assembles and serializes a tunneling confirmation packet into the relay.
sendTunnelReply(Socket, TmpId, TunId, Buffer) ->
	case sendByte(Socket, ?OP_TUN_REP) of
		ok ->
			case sendVarint(Socket, TmpId) of
				ok ->
					case sendVarint(Socket, TunId) of
						ok    -> sendVarint(Socket, Buffer);
						Error -> Error
					end;
				Error -> Error
			end;
		Error -> Error
	end.

%% Assembles and serializes a tunnel data packet into the relay.
sendTunnelData(Socket, TunId, Message) ->
	case sendByte(Socket, ?OP_TUN_DATA) of
		ok ->
			case sendVarint(Socket, TunId) of
				ok    -> sendBinary(Socket, Message);
				Error -> Error
			end;
		Error -> Error
	end.

%% Assembles and serializes a tunnel data acknowledgement into the relay.
sendTunnelAck(Socket, TunId) ->
	case sendByte(Socket, ?OP_TUN_ACK) of
		ok    -> sendVarint(Socket, TunId);
		Error -> Error
	end.

%% Assembles and serializes a tunnel termination message into the relay.
sendTunnelClose(Socket, TunId) ->
	case sendByte(Socket, ?OP_TUN_CLOSE) of
		ok    -> sendVarint(Socket, TunId);
		Error -> Error
	end.

%% Retrieves a single byte from the relay.
recvByte(Socket) ->
	case gen_tcp:recv(Socket, 1) of
		{ok, <<Byte:8>>} -> Byte;
		Error            -> Error
	end.

%% Retrieves a boolean from the relay.
recvBool(Socket) ->
	case recvByte(Socket) of
		Error = {error, _Reason} -> Error;
		0 -> false;
		1 -> true;
		_InvalidBool -> {error, violation}
	end.

%% Retrieves a variable int from the relay.
recvVarint(Socket) ->
	case recvByte(Socket) of
		Error = {error, _Reason} -> Error;
		Byte when Byte < 128     -> Byte;
		Byte ->
			case recvVarint(Socket) of
				Error = {error, _Reason} -> Error;
				Num                      -> Byte - 128 + 128 * Num
			end
	end.

%% Retrieves a length-tagged binary array from the relay.
recvBinary(Socket) ->
	case recvVarint(Socket) of
		Error = {error, _Reason} -> Error;
		Size ->
			case gen_tcp:recv(Socket, Size) of
				{ok, Binary} -> Binary;
				Error        -> Error
			end
	end.

%% Retrieves a length-tagged binary array from the relay.
recvString(Socket) ->
	case recvVarint(Socket) of
		Error = {error, _Reason} -> Error;
		Size ->
			case gen_tcp:recv(Socket, Size) of
				{ok, Binary} -> erlang:binary_to_list(Binary);
				Error        -> Error
			end
	end.

%% Retrieves a connection initialization response and returns whether ok.
procInit(Socket) ->
	case recvByte(Socket) of
		?OP_INIT                 -> ok;
		Error = {error, _Reason} -> Error;
		_InvalidOpcode           -> {error, violation}
	end.

%% Retrieves a remote broadcast message from the relay and notifies the handler.
procBroadcast(Socket, Handler) ->
	case recvBinary(Socket) of
		Error = {error, _Reason} -> Error;
		Msg ->
			Handler ! {broadcast, Msg},
			ok
	end.

%% Retrieves a remote request from the relay.
procRequest(Socket, Owner, Handler) ->
	case recvVarint(Socket) of
		Error = {error, _Reason} -> Error;
		ReqId ->
			case recvBinary(Socket) of
				Error = {error, _Reason} -> Error;
				Req ->
					Handler ! {request, {Owner, ReqId}, Req},
					ok
			end
	end.

%% Retrieves a remote reply to a local request from the relay.
procReply(Socket, Owner) ->
	case recvVarint(Socket) of
		Error = {error, _Reason} -> Error;
		ReqId ->
			case recvBool(Socket) of
				Error = {error, _Reason} -> Error;
				true ->
					Owner ! {reply, ReqId, {error, timeout}},
					ok;
				false ->
					case recvBinary(Socket) of
						Error = {error, _Reason} -> Error;
						Reply ->
							Owner ! {reply, ReqId, {ok, Reply}},
							ok
					end
			end
	end.

%% Retrieves a topic event from the relay.
procPublish(Socket, Handler) ->
	case recvString(Socket) of
		Error = {error, _Reason} -> Error;
		Topic ->
			case recvBinary(Socket) of
				Error = {error, _Reason} -> Error;
				Msg ->
					Handler ! {publish, Topic, Msg},
					ok
			end
	end.

%% Retrieves a remote tunneling request from the relay.
procTunnelRequest(Socket, Owner) ->
	case recvVarint(Socket) of
		Error = {error, _Reason} -> Error;
		TmpId ->
			case recvVarint(Socket) of
				Error = {error, _Reason} -> Error;
				Buffer ->
					Owner ! {tunnel_request, TmpId, Buffer},
					ok
			end
	end.

%% Retrieves the remote reply to a local tunneling request from the relay.
procTunnelReply(Socket, Owner) ->
	case recvVarint(Socket) of
		Error = {error, _Reason} -> Error;
		TunId ->
			case recvBool(Socket) of
				Error = {error, _Reason} -> Error;
				true ->
					Owner ! {tunnel_reply, TunId, {error, timeout}},
					ok;
				false ->
					case recvVarint(Socket) of
						Error = {error, _Reason} -> Error;
						Buffer ->
							Owner ! {tunnel_reply, TunId, {ok, Buffer}},
							ok
					end
			end
	end.

%% Retrieves a remote tunnel data message.
procTunnelData(Socket, Owner) ->
	case recvVarint(Socket) of
		Error = {error, _Reason} -> Error;
		TunId ->
			case recvBinary(Socket) of
				Error = {error, _Reason} -> Error;
				Msg ->
					Owner ! {tunnel_data, TunId, Msg},
					ok
			end
	end.

%% Retrieves a remote tunnel message acknowledgement.
procTunnelAck(Socket, Owner) ->
	case recvVarint(Socket) of
		Error = {error, _Reason} -> Error;
		TunId ->
			Owner ! {tunnel_ack, TunId},
			ok
	end.

%% Retrieves the remote closure of a tunnel.
procTunnelClose(Socket, Owner) ->
	case recvVarint(Socket) of
		Error = {error, _Reason} -> Error;
		TunId ->
			Owner ! {tunnel_close, TunId},
			ok
	end.

%% Retrieves messages from the client connection and keeps processing them until
%% either the relay closes (graceful close) or the connection drops.
process(Socket, Owner, Handler) ->
	Res = case recvByte(Socket) of
		Error = {error, _} -> Error;
		?OP_BCAST          -> procBroadcast(Socket, Handler);
		?OP_REQ            -> procRequest(Socket, Owner, Handler);
		?OP_REP            -> procReply(Socket, Owner);
		?OP_PUB            -> procPublish(Socket, Handler);
		?OP_TUN_REQ        -> procTunnelRequest(Socket, Owner);
		?OP_TUN_REP        -> procTunnelReply(Socket, Owner);
		?OP_TUN_DATA       -> procTunnelData(Socket, Owner);
		?OP_TUN_ACK        -> procTunnelAck(Socket, Owner);
		?OP_TUN_CLOSE      -> procTunnelClose(Socket, Owner);
		?OP_CLOSE          -> exit(ok);
		_InvalidOpcode     -> {error, violation}
	end,
	case Res of
		ok ->
			case process(Socket, Owner, Handler) of
				ok -> ok;
				{error, Reason} -> exit(Reason)
			end;
		{error, Reason} -> exit(Reason)
	end.
