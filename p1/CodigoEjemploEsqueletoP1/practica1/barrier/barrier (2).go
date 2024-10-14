package main

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

func readEndpoints(filename string) ([]string, error) {
	file, err := os.Open(filename) // abre fichero
	if err != nil {                //si hay error
		return nil, err //devuelve error
	}
	defer file.Close() //si no en algún punto lo cierra

	var endpoints []string            //vector de strings
	scanner := bufio.NewScanner(file) //lector de fichero de entrada
	for scanner.Scan() {              //mientras pueda escanear
		line := scanner.Text() //declara la línea como lo leído por el scanner
		if line != "" {        //si la línea no está vacía
			endpoints = append(endpoints, line) //pone al final del vector la línea leída
		}
	}
	if err := scanner.Err(); err != nil { //si hay un error en escaneo
		return nil, err //devuelve el error
	}
	return endpoints, nil //si todo ha ido bien devuelve el vector de endpoints
}

func handleConnection(conn net.Conn, barrierChan chan<- bool, received *map[string]bool, mu *sync.Mutex, n int) {
	defer conn.Close()        //más tarde se cerrará la conexión
	buf := make([]byte, 1024) //crea un bufer de 1024 bytes
	_, err := conn.Read(buf)  //intenta leer del búfer
	if err != nil {           //si hay error al leer
		fmt.Println("Error reading from connection:", err) //lo printea
		return                                             //y vuelve
	}
	msg := string(buf)                                    //convierte el búfer en un string
	mu.Lock()                                             //bloquea el mutex
	(*received)[msg] = true                               //pone el elemento del mapa con clave de lo que ha recibido a true
	fmt.Println("Received ", len(*received), " elements") //imprime "Received x elements depende de cuantos lleve recibidos"
	if len(*received) == n-1 {                            //si ha recibido tantos como endpoints ajenos hay,
		barrierChan <- true //manda true por el canal de la barrera
	}
	mu.Unlock()
}

// Get enpoints (IP adresse:port for each distributed process)
func getEndpoints() ([]string, int, error) {
	endpointsFile := os.Args[1]                 //el fichero endpointsfile es el primer argumeto que le hemos pasados
	var endpoints []string                      // Por qué esta declaración ?
	lineNumber, err := strconv.Atoi(os.Args[2]) // para saber que numero de fila del fichero corresponde
	if err != nil || lineNumber < 1 {           //si hay error al convertir a entero o el número de fila dado es menor que 1
		fmt.Println("Invalid line number") //printea error
	} else if endpoints, err = readEndpoints(endpointsFile); err != nil { //si puede leer el fichero de endpoints
		fmt.Println("Error reading endpoints:", err) //printea error
	} else if lineNumber > len(endpoints) { //y si el número de fila es mayor que el número de lineas del fichero
		fmt.Printf("Line number %d out of range\n", lineNumber) //printea el error
		err = errors.New("line number out of range")            //crea el error para devolverlo después
	}
	return endpoints, lineNumber, err //devuelve el vector de endpoints, el número de línea y el error que haya habido(esperemos nil)
}

func acceptAndHandleConnections(listener net.Listener, quitChannel chan bool,
	barrierChan chan bool, receivedMap *map[string]bool, mu *sync.Mutex, n int) {
	for { //bucle infinito i guess
		select { //escucha de varios canales
		case (<-quitChannel): // si saca algo del canal de quit
			fmt.Println("Stopping the listener...") //printea
			listener.Close()
			return //y sale
		default: //si es un canal normal
			conn, err := listener.Accept() //acepta la conexión
			if err != nil {                //si ha habido error aceptando la conexión
				fmt.Println("Error accepting connection:", err) //lo printea
				continue                                        //y sigue con el bucle
			}
			go handleConnection(conn, barrierChan, receivedMap, mu, n) //y maneja la conexión en particular
		}
	}
}

func notifyOtherDistributedProcesses(endPoints []string, lineNumber int, mu *sync.Mutex, n int, endChan chan bool) {
	messagesSent := 0
	for i, ep := range endPoints { //
		if i+1 != lineNumber {
			go func(ep string) {
				for {
					conn, err := net.Dial("tcp", ep)
					if err != nil {
						fmt.Println("Error connecting to", ep, ":", err)
						time.Sleep(1 * time.Second)
						continue
					}
					_, err = conn.Write([]byte(strconv.Itoa(lineNumber)))
					if err != nil {
						fmt.Println("Error sending message:", err)
						conn.Close()
						continue
					}
					fmt.Println("Message sent to", ep)
					mu.Lock()
					messagesSent++
					if messagesSent == n-1 {
						fmt.Println("All messages sent.")
						endChan <- true
					}
					mu.Unlock()
					conn.Close()
					break
				}
			}(ep)
		}
	}
}

func main() {
	var listener net.Listener

	if len(os.Args) != 3 {
		fmt.Println("Usage: go run main.go <endpoints_file> <line_number>")
	} else if endPoints, lineNumber, err := getEndpoints(); err == nil {
		// Get the endpoint for current process
		localEndpoint := endPoints[lineNumber-1]
		if listener, err = net.Listen("tcp", localEndpoint); err != nil {
			fmt.Println("Error creating listener:", err)
		} else {
			fmt.Println("Listening on", localEndpoint)
		}
		// Barrier synchronization
		var mu sync.Mutex
		var sentMutex sync.Mutex
		quitChannel := make(chan bool)
		receivedMap := make(map[string]bool)
		barrierChan := make(chan bool)
		endChan := make(chan bool)
		n := len(endPoints)

		go acceptAndHandleConnections(listener, quitChannel, barrierChan,
			&receivedMap, &mu, n)

		notifyOtherDistributedProcesses(endPoints, lineNumber, &sentMutex, n, endChan)

		fmt.Println("Waiting for all the processes to reach the barrier")
		<-barrierChan
		<-endChan
		fmt.Println("All processes have reached the barrier.")
		listener.Close()
		quitChannel <- true

	}
}
