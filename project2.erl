-module(project2).
-export([start/3,lineOrFull/5,superviser/3,worker/3,broadcast/2,push_worker/6,broadcastsum/4,grid/6,gridfinal/6]).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start(Nodes,Topology,Algo) -> %main fucntion which starts the program

    case (Topology == two_d) or (Topology == imp_3d) of  %Toplogy = {line,full,two_d,imp_3d} Algo= {gossip,pushsum}
        true -> 
            Gridsize = trunc(math:sqrt(Nodes)),
            register(superviser,spawn(project2,superviser,[[],0,Gridsize*Gridsize])),
            register(grid,spawn(project2,grid,[Gridsize, array:new(Gridsize), Nodes, Gridsize,Topology,Algo]));
        _ ->
            register(superviser,spawn(project2,superviser,[[],0,Nodes])),  
            register(line,spawn(project2,line,[Nodes,array:new(Nodes),Topology,Algo,Nodes]))
    end.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
superviser(Neighbours,N,Nodes) ->  %superviser worker which calculates the convergent times
    receive 
    {nodes,Nodes} ->
        io:format("Nodes:~p~n",[array:to_list(Nodes)]),
        superviser(Neighbours,N,Nodes);
    {rumor,Id} ->
        io:format("Worker ~p coverged, Total:~p~n",[Id,N+1]),
        if 
            N == Nodes-1 -> 
                {_,Time} = statistics(wall_clock),
                io:format("Total Time:~p~n",[Time]);
            true -> superviser(Neighbours,N+1,Nodes)
        end
    end.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
worker(Neigbours,Id,R) -> % worker used for gossip algorithm
    receive
        {neigbours,Neighbour} -> %initializez the worker neighbour
            io:format("Neighbours of worker ~p:~p~n",[Id,Neighbour]),
            worker(Neighbour,Id,R);
        {fullneigbours,Neighbour} -> %avoids printing neighbours in case of full topology because every node is a neighbour
            worker(Neighbour,Id,R);
        {start} ->      %receivest the gossip message
            if 
                R == 10 ->        %checks if rumor count is 10
                    superviser ! {rumor,Id},
                    exit(normal);
                true ->
                    broadcast(Neigbours,10), %broadcasts the rumor to random neighbours
                    worker(Neigbours,Id,R+1)
            end                             
    end.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
push_worker(Neigbours,Id,S,W,C,Converged) -> %worker used for push sum algorithm
    receive
        {neigbours,Neighbour} ->
            io:format("Neighbours of worker ~p:~p~n",[Id,Neighbour]),
            push_worker(Neighbour,Id,S,W,C,Converged);
        {fullneigbours,Neighbour} ->
            push_worker(Neighbour,Id,S,W,C,Converged);

        {start} ->
            Rand = rand:uniform(length(Neigbours)),
            Pid = lists:nth(Rand,Neigbours),
            Pid ! {pushsum,S,W},
            push_worker(Neigbours,Id,S,W,C,Converged);

        {pushsum,Sum,Weight} ->  %receives the push sum from other nodes
            %io:format("worker ~p receieved~n",[Id]),
            %io:format("pushsum~n",[]),
            NewS = S + Sum,
            NewW = W + Weight,
            Diff = abs(S/W - NewS/NewW),
            case Converged == tru of
                true ->
                    Rand = rand:uniform(length(Neigbours)),
                    Pid = lists:nth(Rand,Neigbours),
                    Pid ! {pushsum,NewS/2,NewW/2},
                    %broadcastsum(N,3,NewS/2,NewW/2),
                    push_worker(Neigbours,Id,NewS/2,NewW/2,C,Converged);
                _ ->    
                    case Diff < math:pow(10,-10) of
                true -> 
                    if 
                        C+1 == 3 ->    %checks if ratio is less than 10^-10 for 3 consecutive times and terminates
                            Rand = rand:uniform(length(Neigbours)),
                            Pid = lists:nth(Rand,Neigbours),
                            Pid ! {pushsum,NewS/2,NewW/2}, 
                            superviser ! {rumor,Id},
                            push_worker(Neigbours,Id,NewS/2,NewW/2,C,tru);
                            %exit(normal);
                        true ->         
                            Rand = rand:uniform(length(Neigbours)),
                            Pid = lists:nth(Rand,Neigbours),
                            broadcastsum(Neigbours,20,NewS/2,NewW/2),
                            Pid ! {pushsum,NewS/2,NewW/2},
                            push_worker(Neigbours,Id,NewS/2,NewW/2,C+1,Converged)
                    end;
                _ ->
                    Rand = rand:uniform(length(Neigbours)),
                    Pid = lists:nth(Rand,Neigbours),
                    %broadcastsum(N,10,NewS/2,NewW/2),
                    Pid ! {pushsum,NewS/2,NewW/2},
                    push_worker(Neigbours,Id,NewS/2,NewW/2,0,Converged)
            end    
        end
    end.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
broadcastsum(Neigbours,0,S,W) -> %broadcast function used by pushsum
    ok;
broadcastsum(Neigbours,C,S,W) ->
    Rand = rand:uniform(length(Neigbours)),
    Pid = lists:nth(Rand,Neigbours),
    case is_process_alive(Pid) of 
        true -> 
            Pid ! {pushsum,S,W},
            broadcast(Neigbours,C-1);
        _ -> broadcast(Neigbours,C-1)
    end.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
broadcast(Neigbours,0) ->  %broadcast fucntion used by the gossip
    ok;
broadcast(Neigbours,C) ->
    Rand = rand:uniform(length(Neigbours)),
    Pid = lists:nth(Rand,Neigbours),
    case is_process_alive(Pid) of 
        true -> 
            Pid ! {start},
            broadcast(Neigbours,C-1);
        _ -> broadcast(Neigbours,C-1)
    end.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%    
lineOrFull(0,Array,Topology,Algo,Nodes)  ->  %fucntion which starts the algorithm for line and full topologies
    superviser ! {nodes,Array},
    if 
        Topology == line ->
            neighbourLine(Array,0);  %call the fucntion which initalizes the neigbours of nodes
        Topology == full ->
            neighbourFull(Array,0);
        true -> ok
    end,
    %neighbour(Array,0),
    {_,_} = statistics(wall_clock),
    Rand = rand:uniform(array:size(Array)-1),
    Pid = array:get(Rand,Array),
    Pid ! {start};
lineOrFull(N,Array,Topolgy,Algo,Nodes) ->
    case Algo == gossip of
        true -> 
            Pid = spawn(project2,worker,[[],N-1,0]), 
            ArrayN =  array:set(N-1,Pid,Array),
            lineOrFull(N-1,ArrayN,Topolgy,Algo,Nodes);
        _ -> 
            Pid = spawn(project2,push_worker,[[],N-1,N-1,1,0,fal]),
            ArrayN =  array:set(N-1,Pid,Array),
            lineOrFull(N-1,ArrayN,Topolgy,Algo,Nodes)
    end.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
neighbourLine(Array,N) ->  %computes the neighbours of line topoogy
    Neighbour = [],
    Size = array:size(Array),
    if 
        N == 0 -> 
            Neighbourn = lists:append(Neighbour,[array:get(N+1,Array)]),
            array:get(N,Array) ! {neigbours,Neighbourn};
        N == Size-1 ->
            Neighbourn = lists:append(Neighbour,[array:get(N-1,Array)]),
            array:get(N,Array) ! {neigbours,Neighbourn};
        true ->
            Neighbourn = lists:append(Neighbour,[array:get(N+1,Array),array:get(N-1,Array)]),
            array:get(N,Array) ! {neigbours,Neighbourn}
    end,
    case N == Size-1 of
        true -> ok;
        false -> neighbourLine(Array,N+1)

    end.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
neighbourFull(Array,N) -> %computes the neighbours of full topology
    Size = array:size(Array),
    array:get(N,Array) ! {fullneigbours,lists:delete(array:get(N,Array),array:to_list(Array))},
    case N == Size-1 of
        true -> ok;
        false -> neighbourFull(Array,N+1)

    end.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
neighbour2D(FinalArray,N, Gridsize,Topology) -> %computes the neighbours of 2D topology
    Neighbour = [],
    
    I = trunc(N/Gridsize),
    %io:format(" ~p ~n", [I]),
    J = N rem Gridsize,
    %io:format(" ~p ~n", [J]),

    if 
        
        (I >= 1) and (J >= 1) and (I =< (Gridsize - 2)) and (J =< (Gridsize - 2)) ->
            Neighbourn = lists:append(Neighbour,[array:get(J+1,array:get(I, FinalArray)), array:get(J-1,array:get(I, FinalArray)), array:get(J,array:get(I-1, FinalArray)), array:get(J,array:get(I+1, FinalArray))]),
            twoD(FinalArray,I,J,Neighbourn,Topology);    

        (I == 0) and (J == 0)  ->
            Neighbourn = lists:append(Neighbour,[array:get(J+1,array:get(I, FinalArray)), array:get(J,array:get(I+1, FinalArray))]),
           twoD(FinalArray,I,J,Neighbourn,Topology);

        (I == Gridsize-1) and (J == 0)  ->
            Neighbourn = lists:append(Neighbour,[array:get(J+1,array:get(I, FinalArray)), array:get(J,array:get(I-1, FinalArray))]),
            twoD(FinalArray,I,J,Neighbourn,Topology);

        (I == 0) and (J == Gridsize-1)  ->
            Neighbourn = lists:append(Neighbour,[array:get(J-1,array:get(I, FinalArray)), array:get(J,array:get(I+1, FinalArray))]),
            twoD(FinalArray,I,J,Neighbourn,Topology);

        (I == Gridsize-1) and (J == Gridsize-1)  ->
            %io:format(" I am Here", [] ),
            Neighbourn = lists:append(Neighbour,[array:get(J,array:get((I-1), FinalArray)), array:get(J-1,array:get(I, FinalArray))]),
            %io:format(" Hello!", [] ),
           twoD(FinalArray,I,J,Neighbourn,Topology);

        (I == 0) and (J >= 1) and (J =< (Gridsize - 2)) ->
            Neighbourn = lists:append(Neighbour,[array:get(J+1,array:get(I, FinalArray)), array:get(J-1,array:get(I, FinalArray)), array:get(J,array:get(I+1, FinalArray))]),
            twoD(FinalArray,I,J,Neighbourn,Topology);

        (I >= 1) and (J == 0) and (I =< (Gridsize - 2)) ->
            Neighbourn = lists:append(Neighbour,[array:get(J+1,array:get(I, FinalArray)), array:get(J,array:get(I-1, FinalArray)), array:get(J,array:get(I+1, FinalArray))]),
            twoD(FinalArray,I,J,Neighbourn,Topology);

        (I == (Gridsize-1)) and (J >= 1) and (J =< (Gridsize - 2)) ->
            Neighbourn = lists:append(Neighbour,[array:get(J+1,array:get(I, FinalArray)), array:get(J-1,array:get(I, FinalArray)), array:get(J,array:get(I-1, FinalArray))]),
            twoD(FinalArray,I,J,Neighbourn,Topology);

        (I >= 1) and (J == (Gridsize - 1)) and (I =< (Gridsize - 2)) ->
            %io:format(" hello  ~p ~n", [N] ),
            Neighbourn = lists:append(Neighbour, [array:get(J-1,array:get(I, FinalArray)), array:get(J,array:get(I-1, FinalArray)), array:get(J,array:get(I+1, FinalArray))]),
            twoD(FinalArray,I,J,Neighbourn,Topology);

        true ->
            ok
    end,
    case N == 0 of
        true -> ok;
        false -> neighbour2D(FinalArray,N-1, Gridsize,Topology)

    end.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
twoD(FinalArray,I,J,Neighbourn,Topology) -> %helper fucntion used to calculate imperfect 3d neighbours
    case Topology == imp_3d of
                true -> 
                    %io:format("Final Array:~p~n",[FinalArray]),
                    ArrayList = lists:foldl(fun(X,B)-> lists:append(B,array:to_list(X)) end, [],array:to_list(FinalArray)),
                    %io:format("Final Array list :~p~n",[ArrayList]),
                    ImpNeighbours = lists:subtract(ArrayList,Neighbourn),                 
                    %io:format("Neighbour:~p~n",[Neighbourn]),
                    %io:format("ImpNeigh:~p~n",[ImpNeighbours]),
                    %io:format("nth:~p~n",[lists:nth(rand:uniform(length(ImpNeighbours)),ImpNeighbours)]),
                    NewNeighbours = lists:append(Neighbourn,[lists:nth(rand:uniform(length(ImpNeighbours)),ImpNeighbours)]),
                    array:get(J, array:get(I, FinalArray))! {neigbours,NewNeighbours};
                _ ->
                    array:get(J, array:get(I, FinalArray))! {neigbours,Neighbourn}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
grid(0, FinalArray, Number, Gridsize,Topology,Algo)  ->
    superviser ! {nodes,FinalArray},
    neighbour2D(FinalArray, Number -1, Gridsize,Topology),   %15 = Number -1, 4 = Gridsize
    {_,_} = statistics(wall_clock),
    array:get(0,array:get(0, FinalArray)) ! {start};
 
grid(Num, FinalArray, Number, Gridsize,Topology,Algo) ->

    Column =  gridfinal(Gridsize, Num, array:new(Gridsize), Gridsize,Topology,Algo),
    ArrayN = array:set(Num-1, Column, FinalArray),
    grid(Num-1, ArrayN, Number, Gridsize,Topology,Algo ).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

gridfinal(0, Row, FinalArray, Gridsize,Topology,Algo)  ->
    FinalArray;
 
gridfinal(Num,Row, FinalArray, Gridsize,Topology,Algo) ->
    case Algo == gossip of
        true ->
            Pid = spawn(project2,worker,[[],(Gridsize*(Row-1)) + (Num-1),0]),
            ArrayNew =  array:set(Num-1,Pid, FinalArray),
            gridfinal(Num-1,Row, ArrayNew, Gridsize,Topology,Algo);
        _ -> 
            Pid = spawn(project2,push_worker,[[],(Gridsize*(Row-1)) + (Num-1),(Gridsize*(Row-1)) + (Num-1),1,0,fal]),
            ArrayNew =  array:set(Num-1,Pid, FinalArray),
            gridfinal(Num-1,Row, ArrayNew, Gridsize,Topology,Algo)
    end.