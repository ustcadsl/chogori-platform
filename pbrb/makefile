all: indexer.o pbrb.o schema.o test.cpp
	g++ -std=c++17 -g indexer.o pbrb.o schema.o test.cpp -o test.out

pbrb.o: PbrbAlgo.cpp pbrb_design.h plog.h
	g++ -std=c++17 -g -c PbrbAlgo.cpp -o pbrb.o 

indexer.o: indexer.cpp indexer.h
	g++ -std=c++17 -g -c indexer.cpp -o indexer.o

schema.o: schema.cpp schema.h
	g++ -std=c++17 -g -c schema.cpp -o schema.o

clean:
	rm *.out *.o