% engine.pl - prolog interface to the HPCE
% Copyright 2015, Simularity, Inc

:- module(engine, [
		   engine_connect/3,
		   engine_close/1,
		   engine_apply/6,
		   engine_delete/3,
		   engine_flush/1,
		   engine_get_type/3,
		   engine_get_object/4,
		   engine_delete_expr/4,
		   engine_delete_item/4,
		   engine_synchronize/1
		  ]).

:- use_module(library(http/http_header)).
:- use_module(library(http/http_client)).
:- use_module(library(http/json)).
:- use_module(library(socket)).
:- use_module(library(barriers)).
:- use_module(library(term_set)).

:- dynamic segment/6.
:- dynamic type_def/2.
:- dynamic obj_def/3.
:- dynamic action_def/2.
:- dynamic connection/5.

:- thread_local action/1.

:- multifile apply_action/6.

safe_close(Str) :-
	catch(close(Str), _, true).

engine_close(Name) :-
	findall(term(Host, Sport, Queue),
		segment(index(Name, _Seg), Host, _Port, Sport, _Thread, Queue),
		TermList),

	retractall(segment(index(Name, _), _, _, _, _, _)),
	retractall(type_def(index(Name, _), _)),
	retractall(obj_def(index(Name, _), _)),
	retractall(action_def(index(Name, _), _)),
	retractall(connection(Name, _, _, _)),
	maplist(term_thread, TermList),
	barrier_delete(Name).

term_thread(term(Host, Sport, Q)) :-
	transact(Host, Sport, Q),
	term_set_delete(Q).

engine_connect(Name, Host, Port) :-

	format(atom(URL), 'http://~w:~w/segments', [Host, Port]),
	
	http_get(URL, JSON, []),
	atom_json_term(JSON, JTerm, []),
	JTerm = json(JSONList),
	member(segments=JList, JSONList),
	(catch(set_segments(JList, Name), Exception,
	       (format('~w~n', [Exception]), fail)) ->
	 length(JList, NSegs),
	 assertz(connection(Name, Host, Port, NSegs)),
	 BCount is NSegs + 1,
	 barrier_create(Name, BCount)
	;
	 engine_close(Name),
	 fail).

engine_synchronize(Name) :-
	synchronize(Name).
synchronize(Name) :-
	findall(sync(Host, Sport, Queue), segment(index(Name, _), Host, _, Sport, _Thread, Queue),
		QueueList),

	maplist(send_sync, QueueList),
	barrier_wait(Name, _).

send_sync(sync(Host, Sport, Queue)) :-
	transact(Host, Sport, Queue).

set_segments([], _Name).
set_segments([json(List) | T], Name) :-
	format('~w~n', [List]),
	member(segment=Seg, List),
	member(host=Host, List),
	member(port=Port, List),
	member(sport=Sport, List),
	term_set_create(Queue),
	% TID is a holdover
	assertz(segment(index(Name, Seg), Host, Port, Sport, _TID, Queue)),
	!,
	set_segments(T, Name).

transact(Host, Port, TS) :-
	!,
	format(atom(URL), 'http://~w:~w/load_data', [Host, Port]),
	term_set_bind(TS, Codes),
	term_set_clear(TS),
	catch(http_post(URL, codes(Codes), _Result, []), X, (!, sleep(0.5), format('ERROR: ~w~n', [X]), 
							     transact(Host, Port, TS))).

accumulate(Host, Port, Message, TS) :-
	test_message(Message, TS),
	% transact fails or succeeds based on final. terminate will succeed

	transact(Host, Port, TS).

% Always succeed, just don't always transact
accumulate(_Host, _Port, _Message, _TS).

% Add test_message values if you want to manipulate the elements
test_message(X, TS) :-
	X =.. [triple | _],
	term_set_add(TS, X),
	random_between(0, 10000, 0).
%	count_to_n(10000).

:- thread_local counter/1.

count_to_n(N) :-
	(counter(M) ->
	 retract(counter(M))
	;
	 M = 0),
	K is M + 1,
	(N = K ->
	 true
	;
	 asserta(counter(K)),
	 fail).

sync(Name) :-
	barrier_wait(Name, _),
	fail.

get_action(Name, Action, Code) :-
	assure_connection(Name), 
	(action_def(index(Name, Action), Code) ->
	 true
	;
	 connection(Name, Host, Port, _NSegs),
	 format(atom(URL), 'http://~w:~w/convert_action?name=~w',
		[Host, Port, Action]),

	 http_get(URL, JDoc, []),
	 
	 ((atom_json_term(JDoc, JSon, []), JSon = json(JList),
	   member(status=0, JList), member(id=Code, JList)) ->
	  asserta(action_def(index(Name, Action), Code))
	 ;
	  throw(error(web_transaction(Name, URL), get_action/3)))).


get_object(Name, Type, ObjName, Code) :-
	assure_connection(Name), 
	Type =.. [Class, TypeNum],
	(atom(TypeNum) ->
	 get_type(Name, Type, TypeId)
	;
	 TypeNum = TypeId),
	TypeSpec =.. [Class, TypeId],
	(obj_def(index(Name, ObjName), TypeSpec, Code) ->
	 true
	;
	 connection(Name, Host, Port, _NSegs),
	 uri_encoded(query_value, ObjName, Encoded), 
	 format(atom(URL), 'http://~w:~w/convert_object?name=~w&class=~w&type=~w', [Host, Port, Encoded, Class, TypeId]),

	 http_get(URL, JDoc, []),

	 ((atom_json_term(JDoc, Json, []),
	   Json=json(JList), member(status=0, JList),
	   member(id=Code, JList)) ->

	  asserta(obj_def(index(Name, ObjName), TypeSpec, Code))
	 ;
	  throw(error(web_transaction(Name, URL), get_object/4)))).

engine_get_type(Name, Type, Code) :-
	get_type(Name, Type, Code).

engine_get_object(Name, Type, ObjName, Code) :-
	get_object(Name, Type, ObjName, Code).

get_type(Name, Type, Code) :-
	assure_connection(Name),
	Type =.. [Class, TypeId],
	(resolve_type(Name, Type, Code) ->
	 true
	;
	 connection(Name, Host, Port, _NSegs),
	 format(atom(URL), 'http://~w:~w/convert_type?class=~w&name=~w', [Host, Port, Class, TypeId]),
	 http_get(URL, JDoc, []),
	 ((atom_json_term(JDoc, JSON, []),
	   JSON=json(JList), member(status=0, JList),
	   member(id=Code, JList)) ->
	  assertz(type_def(index(Name, Type), Code))
	 ;
	  throw(error(web_transaction(Name, URL), get_type/3)))).

resolve_type(Name, Type, Code) :-
	Type =.. [_Class, TypeSpec],
	(atom(TypeSpec) ->
	 type_def(index(Name, Type), Code)
	;
	 Code = TypeSpec).

engine_apply(Name, SType, SObj, Action, OType, OObj) :-
	assure_connection(Name),
	catch(engine_apply_(Name, SType, SObj, Action,
			    OType, OObj),
	      Exception,
	      (engine_close(Name), throw(Exception))).

engine_apply_(Name, SType, SObj, Action, OType, OObj):-
	(atom(SType) ->
	 get_type(Name, subject(SType), STypeId)
	;
	 STypeId = SType),

	(atom(OType) ->
	 get_type(Name, object(OType), OTypeId)
	;
	 OTypeId = OType),

	(atom(SObj) ->
	 get_object(Name, subject(STypeId), SObj, SObjId)
	;
	 SObjId = SObj),

	(atom(OObj) ->
	 get_object(Name, object(OTypeId), OObj, OObjId)
	;
	 OObjId = OObj),

	(atom(Action) ->
	 get_action(Name, Action, ActionId)
	;
	 ActionId = Action),
	
	connection(Name, _, _, NSegs),

	OSeg is floor(OObjId) mod NSegs,
	SSeg is floor(SObjId) mod NSegs,
	segment(index(Name, OSeg), OHost, _, OSport, _OThr, OQ),
	segment(index(Name, SSeg), SHost, _, SSport, _SThr, SQ),

	accumulate(OHost, OSport, triple(subject(STypeId, SObjId),
					 ActionId,
					 object(OTypeId, OObjId)), OQ),
	
	accumulate(SHost, SSport, triple(object(OTypeId, OObjId),
					 ActionId,
					 subject(STypeId, SObjId)), SQ),
	(apply_action(Name, STypeId, SObjId, ActionId, OTypeId,
		      OObjId) ->
	 true
	;
	 true), !.

map_types(subject(ST, SO), object(OT, OO),
	  subject(ST, SO), object(OT, OO)) :- !.

map_types(object(OT, OO), subject(ST, SO),
	  subject(ST, SO), object(OT, OO)) :- !.

map_types(A, B, _, _) :-
	throw(error(type_error(subject-object, [A, B]),
		    map_types/4)).

engine_delete(Name, First, Second) :-
	assure_connection(Name),
	map_types(First, Second, Subject, Object),
	Subject = subject(SType, SObj),
	Object = object(OType, OObj),
	catch(engine_delete_(Name, SType, SObj,
			    OType, OObj),
	      Exception,
	      (engine_close(Name), throw(Exception))).

engine_delete_(Name, SType, SObj, OType, OObj):-
	(atom(SType) ->
	 get_type(Name, subject(SType), STypeId)
	;
	 STypeId = SType),

	(atom(OType) ->
	 get_type(Name, object(OType), OTypeId)
	;
	 OTypeId = OType),

	(atom(SObj) ->
	 get_object(Name, subject(STypeId), SObj, SObjId)
	;
	 SObjId = SObj),

	(atom(OObj) ->
	 get_object(Name, object(OTypeId), OObj, OObjId)
	;
	 OObjId = OObj),

	connection(Name, _, _, NSegs),

	OSeg is OObjId mod NSegs,
	SSeg is SObjId mod NSegs,
	segment(index(Name, OSeg), OHost, _, OSport, _OThr, OQ),
	segment(index(Name, SSeg), SHost, _, SSport, _SThr, SQ),

	
	accumulate(OHost, OSport, triple(subject(STypeId, SObjId),
					 object(OTypeId, OObjId)), OQ),
	accumulate(SHost, SSport, triple(object(OTypeId, OObjId),
					 subject(STypeId, SObjId)), SQ),
	!.
assure_connection(Name) :-
	connection(Name, _, _, _), !.

assure_connection(Name) :-
	throw(error(connection(Name), _)).


engine_flush(Name) :-
	engine_synchronize(Name).

engine_delete_expr(Name, Expr, ActionList, TypeList) :-
	maplist(url_action, ActionList, ActPart),
	maplist(url_types, TypeList, TypePart),
	atomic_list_concat(ActPart, '&', ActAtom),
	atomic_list_concat(TypePart, '&', TypeAtom),

	connection(Name, Host, Port, _Segs),
	format(atom(URL), 'http://~w:~w/expr_receivers?~w&~w', [Host, Port, ActAtom, TypeAtom]),
	format_to_codes('~q.', [Expr], Codes), 
	repeat,
	
	catch(http_post(URL, codes(Codes), Result, []), _,
	      (!, sleep(0.5),
	       engine_delete_expr(Name, Expr, ActionList, TypeList))),
	atom_json_term(Result, Json, []), 
	(Json = json([status=0, receivers=JsonRecv])
	->
	 maplist(json_receiver, JsonRecv, Receivers)
	;
	 throw(error(type_error(receiver_json, Json),
		     engine_delete/3))),
	
	maplist(engine_delete_item(Name, ActionList,
				   TypeList),
		Receivers),

	sleep(0.5),
	Receivers = [],
	!.

url_action(forward(Action), Atom) :-
	atomic(Action),
	atom_term(Atom, faction=Action), !.
url_action(reverse(Action), Atom) :-
	atomic(Action),
	atom_term(Atom, raction=Action), !.
url_action(Action, Atom) :-
	atomic(Action),
	atom_term(Atom, action=Action), !.

url_action(Action, _) :-
	throw(error(type_error(action, Action),
		    url_action/2)).

url_types(subject(Stype), Atom) :-
	atomic(Stype),
	atom_term(Atom, stype=Stype), !.

url_types(object(Otype), Atom) :-
	atomic(Otype),
	atom_term(Atom, otype=Otype), !.

url_types(Obj, _) :-
	throw(error(type_error(sim_object, Obj),
		    url_types/2)).

atom_term(Atom, Term) :-
	format(atom(Atom), '~w', [Term]).

json_receiver(json(Json), Object) :-
	member(class=Class, Json),
	member(type=Type, Json),
	member(item=Item, Json),
	Object =.. [Class, Type, Item], !.

json_receiver(Json, _) :-
	throw(error(type_error(receiver_json, Json),
		    json_receiver/2)).

engine_delete_item(Name, ActionList, TypeList, Obj) :-

	(member(Obj, [subject(_, _), object(_, _)]) ->
	 true
	;
	 throw(error(type_error(sim_object, Obj),
		     engine_delete_item/6))),

	maplist(url_action, ActionList, ActPart),
	maplist(url_types, TypeList, TypePart),
	atomic_list_concat(ActPart, '&', ActAtom),
	atomic_list_concat(TypePart, '&', TypeAtom),

	connection(Name, Host, Port, _Segs),
	format(atom(URL), 'http://~w:~w/expr_receivers?~w&~w', [Host, Port, ActAtom, TypeAtom]),
	format_to_codes('~q.', [Obj], Codes),
	repeat,
	
	catch(http_post(URL, codes(Codes), Result, []), _,
	      (!, sleep(0.5),
	       engine_delete_item(Name, ActionList, TypeList, Obj))),
	atom_json_term(Result, Json, []),
	(Json = json([status=0, receivers=JsonRecv])
	->
	 maplist(json_receiver, JsonRecv, Receivers)
	;
	 throw(error(type_error(receiver_json, Json),
		     engine_delete_item/3))),
	
	maplist(engine_delete(Name, Obj), Receivers),
	synchronize(Name),
	Receivers = [],
	!.

