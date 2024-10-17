/*
* AUTOR: Rafael Tolosana Calasanz
* ASIGNATURA: 30221 Sistemas Distribuidos del Grado en Ingeniería Informática
*			Escuela de Ingeniería y Arquitectura - Universidad de Zaragoza
* FECHA: septiembre de 2021
* FICHERO: ricart-agrawala.go
* DESCRIPCIÓN: Implementación del algoritmo de Ricart-Agrawala Generalizado en Go
 */
package ra

import (
	"fmt"
	"math"
	"practica2/ms"
	"sync"
	"time"
)

const MAXPROCESSES = 4

type Request struct {
	Clock []int
	Pid   int
	Task  string
}

type Reply struct {
	Pid int
}

type RASharedDB struct {
	N          int
	HighSeqNum int
	Me         int
	Clock      []int
	OutRepCnt  int
	AwReps     []bool
	ReqCS      bool
	RepDefd    []bool
	Ms         *ms.MessageSystem
	done       chan bool
	Chrep      chan bool
	Task       string
	Mutex      sync.Mutex // mutex para proteger concurrencia sobre las variables
	// TODO: completar
}

// MAPA que se usará como matriz de exclusión
var exclusionMatrix = map[string]map[string]bool{
	"LECTURA": {
		"LECTURA":   false,
		"ESCRITURA": true,
	},
	"ESCRITURA": {
		"LECTURA":   true,
		"ESCRITURA": true,
	},
}

//función para comprobar si puedo conceder permiso a una solicitud
//func (ra *RASharedDB) puedeConcederPermiso(operacionActual, operacionSolicitada string) bool {
//    return !exclusionMatrix[operacionActual][operacionSolicitada]
//}

func New(me int, usersFile string, task string) *RASharedDB {
	messageTypes := []ms.Message{Request{}, Reply{}}
	msgs := ms.New(me, usersFile, messageTypes)
	ra := RASharedDB{MAXPROCESSES, 0, me, make([]int, MAXPROCESSES), 0, make([]bool, MAXPROCESSES), false, make([]bool, MAXPROCESSES), &msgs, make(chan bool), make(chan bool), task, sync.Mutex{}}
	for i := range ra.Clock {
		ra.Clock[i] = 0
	}
	for i := range ra.RepDefd {
		ra.RepDefd[i] = false
	}

	// TODO completar
	go messageHandler(&ra)
	return &ra
}

// Pre: Verdad
// Post: Realiza  el  PreProtocol  para el  algoritmo de
//
//	Ricart-Agrawala Generalizado
func (ra *RASharedDB) PreProtocol() {
	var req Request
	ra.Mutex.Lock()
	ra.ReqCS = true
	ra.Clock[ra.Me-1] = ra.HighSeqNum + 1
	req = Request{ra.Clock, ra.Me, ra.Task}
	for i := 0; i < ra.N; i++ {
		if i+1 != ra.Me {
			ra.AwReps[i] = true
			ra.RepDefd[i] = false
			fmt.Println("Mandando solicitud al proceso", i+1)
			time.Sleep(3 * time.Second)
			ra.Ms.Send(i+1, req)
			ra.OutRepCnt++
		}
	}
	ra.Mutex.Unlock()
	<-ra.Chrep
}

func messageHandler(ra *RASharedDB) {
	for {
		msg := ra.Ms.Receive()
		switch msg := msg.(type) {
		case Request:
			handleRequest(&msg, ra)
		case Reply:
			handleReply(&msg, ra)
		}
	}
}

func maxV(v []int) int {
	var m int
	for i, e := range v {
		if i == 0 || e > m {
			m = e
		}
	}
	return m
}

func handleRequest(r *Request, ra *RASharedDB) {
	var deferr bool //Esta variable determina si la respuesta se aplazará o se dará de inmediato
	ra.Mutex.Lock()
	if (exclusionMatrix[ra.Task])[r.Task] {
		if !ra.ReqCS {
			deferr = false
		} else {
			if isVectorLesser(ra.Clock, r.Clock) {
				deferr = true
			} else {
				if isVectorGreater(ra.Clock, r.Clock) { //Si mi reloj es mayor
					//Enviar respuesta
					deferr = false
				} else {
					//Comparamos id
					if ra.Me < r.Pid {
						deferr = true
					} else {
						deferr = false
					}
				}
			}
		}
	} else {
		deferr = false
	}
	if deferr {
		ra.RepDefd[r.Pid-1] = true
	} else {
		rep := Reply{ra.Me}
		ra.Ms.Send(r.Pid, rep)
	}
	for i := 0; i < len(ra.Clock); i++ {
		ra.Clock[i] = int(math.Max(float64(ra.Clock[i]), float64(r.Clock[i])))
	}
	ra.HighSeqNum = maxV(ra.Clock)
	ra.Mutex.Unlock()
}

func handleReply(r *Reply, ra *RASharedDB) {
	fmt.Println("Recibida respuesta de", r.Pid)
	ra.Mutex.Lock()
	defer ra.Mutex.Unlock()
	if ra.AwReps[r.Pid-1] {
		ra.OutRepCnt--

		if ra.OutRepCnt == 0 {
			fmt.Println("Todas las respuestas han sido recibidas...")
			ra.Chrep <- true
		}
	}
}

func isVectorLesser(v1 []int, v2 []int) bool {
	if len(v1) != len(v2) {
		return false
	}
	for i := 0; i < len(v2); i++ {
		if v1[i] > v2[i] {
			return false
		}
	}
	return true
}

func isVectorGreater(v1 []int, v2 []int) bool {
	if len(v1) != len(v2) {
		return false
	}
	for i := 0; i < len(v2); i++ {
		if v1[i] < v2[i] {
			return false
		}
	}
	return true
}

/*
func isVectorConcurrent(v1 []int, v2 []int) bool {
	return !(isVectorGreater(v1, v2) || isVectorLesser(v1, v2))
}
*/

// Pre: Verdad
// Post: Realiza  el  PostProtocol  para el  algoritmo de
//
//	Ricart-Agrawala Generalizado
func (ra *RASharedDB) PostProtocol() {
	ra.Mutex.Lock()
	for i, def := range ra.RepDefd {
		if def {
			rep := Reply{ra.Me}
			ra.Ms.Send(i+1, rep)
		}
	}
	ra.Mutex.Unlock()
}

func (ra *RASharedDB) Stop() {
	ra.Ms.Stop()
	ra.done <- true
}
