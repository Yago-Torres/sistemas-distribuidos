package main

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
	"raft/internal/comun/check"
	"raft/internal/comun/rpctimeout"
	"raft/internal/raft"
	"strconv"
)

func main() {
	// obtener entero de índice de este nodo
	me, err := strconv.Atoi(os.Args[1])
	check.CheckError(err, "Main, mal número entero de índice de nodo:")

	var nodos []rpctimeout.HostPort
	// Resto de argumento son los end points como strings
	// De todas la replicas-> pasarlos a HostPort
	for _, endPoint := range os.Args[2:] {
		nodos = append(nodos, rpctimeout.HostPort(endPoint))
	}

	// Parte Servidor
	nr := raft.NuevoNodo(nodos, me, make(chan raft.AplicaOperacion, 1000))
	rpc.Register(nr)

	fmt.Println("Replica escucha en :", me, " de ", os.Args[2])

	// Escuchar en el endpoint del nodo actual (os.Args[2] corresponde al endpoint correcto)
	l, err := net.Listen("tcp", os.Args[2])
	check.CheckError(err, "Main listen error:")

	rpc.Accept(l)
}
