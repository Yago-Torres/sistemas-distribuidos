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
    "math"
    "ms"
    "sync"
)

const MAXPROCESSES = 4

type Request struct{
    Clock   int[]
    Pid     int
}

type Reply struct{
    Pid     int
}

type RASharedDB struct { 
    N           int
    HighSeqNum  int
    Me          int
    Clock       []int
    OutRepCnt   int
    AwReps      []bool
    ReqCS       bool
    RepDefd     []bool
    Ms          *MessageSystem
    done        chan bool
    Chrep       chan bool
    Mutex       sync.Mutex // mutex para proteger concurrencia sobre las variables
    // TODO: completar
}

// MAPA que se usará como matriz de exclusión
var exclusionMatrix = map[string]map[string]bool{
    "LECTURA": {
        "LECTURA": false,
        "ESCRITURA": true,
    },
    "ESCRITURA": {
        "LECTURA": true,
        "ESCRITURA": true,
    },
}

//función para comprobar si puedo conceder permiso a una solicitud
//func (ra *RASharedDB) puedeConcederPermiso(operacionActual, operacionSolicitada string) bool {
//    return !exclusionMatrix[operacionActual][operacionSolicitada]
//}


func New(me int, usersFile string) (*RASharedDB) {
    messageTypes := []Message{Request, Reply}
    msgs = ms.New(me, usersFile string, messageTypes)
    ra := RASharedDB{MAXPROCESSES,0, me, make([]int,N),0, &msgs,  make(chan bool),  make(chan bool), &sync.Mutex{}}
    // TODO completar
    go messageHandler(me, &ra)
    return &ra
}

//Pre: Verdad
//Post: Realiza  el  PreProtocol  para el  algoritmo de
//      Ricart-Agrawala Generalizado
func (ra *RASharedDB) PreProtocol(){
    var req Request
    ra.Mutex.Lock()
    ra.ReqCs = true
    ra.Clock[ra.me]=ra.HighSeqNum++;
    req = {ra.me, ra.Clock}
    for(int i=0;i<ra.N;i++) {
        if(i!=ra.me) {
            ra.AwRps[i]=true;
            ra.ms.Send(i+1, req)
            ra.OutRepCnt++
        }
    }
    ra.Mutex.Unlock()
    <- Chrep
}

func messageHandler(me int, ra *RASharedDB) {
    for {
        msg := ra.ms.Receive()
        switch m:= msg.(type) {
            case Request:
                handleRequest(&msg, ra)
            case Reply:
                handleReply(&msg,ra)
        }
    }

}

func maxV(v []int) {
    var m int
    for i, e := range v {
        if i==0 || e > m {
            m = e
        }   
    }
    return m
}

func handleRequest(r *Request,ra *RASharedDB) {
    var defer bool
    ra.Mutex.Lock()
    if(!ra.ReqCS) {
        defer = false
    }
    else {
        if(isVectorLesser(ra.Clock, r.clock)) {
            defer = true
        }
        else {
            if(isVectorGreater(ra.Clock, r.clock)) { //Si mi reloj es mayor
                //Enviar respuesta
                defer = false
            }
            else {
                //Comparamos id
                if(ra.me < r.Id) {
                    defer = true
                }
                else {
                    defer = false
                }
            }
        }
    }
    if defer {
        ra.RepDefd[r.Id-1] = true
    }
    else {
        var rep Reply
        rep := {ra.me}
        ra.ms.Send(r.Id, rep)
    }
    for i := 0; i < len(ra.Clock); i++ {
        ra.Clock[i] = Math.max(ra.Clock[i], r.Clock[i])
    }
    ra.HighSeqNum := maxv(ra.Clock)
    ra.Mutex.Unlock()
}

func handleReply(r *Reply,ra *RASharedDB) {
    ra.Mutex.Lock()
    defer ra.Mutex.Unlock()
    if !ra.AwReps[r.Id] {
        ra.OutRepCnt--
        if ra.OutRepCnt == 0{
            chrep <- true
        }
    }
}

func isVectorLesser(v1 int[], v2 int[]) bool{
    if len(v1) != len(v2) {
        return false
    }
    for(int i=0;i<len(v2);i++) {
        if(v1[i] >= v2[i]) {
            return false
        }
    }
    return true
}

func isVectorGreater(v1 []int, v2 []int) bool{
    if len(v1) != len(v2) {
        return false
    }
    for(int i=0;i<len(v2);i++) {
        if(v1[i] <= v2[i]) {
            return false
        }
    }
    return true
}

function isVectorConcurrent(v1 []int, v2 []int) {
    return !(isVectorGreater(v1,v2) || isVectorLesser(v1,v2))
}

//Pre: Verdad
//Post: Realiza  el  PostProtocol  para el  algoritmo de
//      Ricart-Agrawala Generalizado
func (ra *RASharedDB) PostProtocol(){
    ra.Mutex.Lock()
    for i, def := range ra.RepDefd {
        if def {
            var rep Reply
            rep = {ra.Me}
            ra.ms.Send(i+1,rep)
        }
    }

    re.Mutex.Unlock()
}

func (ra *RASharedDB) Stop(){
    ra.ms.Stop()
    ra.done <- true
}
