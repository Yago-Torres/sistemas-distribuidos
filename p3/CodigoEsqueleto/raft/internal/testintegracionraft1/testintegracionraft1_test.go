package testintegracionraft1

import (
	"fmt"
	"raft/internal/comun/check"

	//"log"
	//"crypto/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"raft/internal/comun/rpctimeout"
	"raft/internal/despliegue"
	"raft/internal/raft"
)

const (
	//nodos replicas
	REPLICA1 = "127.0.0.1:29001"
	REPLICA2 = "127.0.0.1:29002"
	REPLICA3 = "127.0.0.1:29003"
	//REPLICA1 = "192.168.3.4:29250"
	//REPLICA2 = "192.168.3.2:29250"
	//REPLICA3 = "192.168.3.3:29250"
	// paquete main de ejecutables relativos a directorio raiz de modulo
	//EXECREPLICA = "/home/yago/Desktop/unizar24-25/sisdist/p3/CodigoEsqueleto/raft/cmd/srvraft/main.go"
	EXECREPLICA = "/home/yago/Desktop/unizar24-25/sisdist/p3/CodigoEsqueleto/raft/cmd/srvraft/main.go"

	// comando completo a ejecutar en máquinas remota con ssh. Ejemplo :
	// 				cd $HOME/raft; go run cmd/srvraft/main.go 127.0.0.1:29001
)

/*
var primeraInicializacion bool = true
*/
// PATH de los ejecutables de modulo golang de servicio Raft
var cwd, _ = os.Getwd()
var PATH string = filepath.Dir(filepath.Dir(cwd))

var path string = "/home/yago/Desktop/unizar24-25/sisdist/p3/CodigoEsqueleto/raft"

//var path string = "/home/a878417/distribuidos/practica3/raft"

// go run cmd/srvraft/main.go 0 127.0.0.1:29001 127.0.0.1:29002 127.0.0.1:29003
var EXECREPLICACMD string = "cd " + path + "; /usr/local/go/bin/go run " + EXECREPLICA

// ////////////////////////////////////////////////////////////////////////////
// /////////////////////			 FUNCIONES TEST
// ///////////////////////////////////////////////////////////////////////////

// TEST primer rango

func TestPrimerasPruebas(t *testing.T) { // (m *testing.M) {
	// <setup code>
	// Crear canal de resultados de ejecuciones ssh en maquinas remotas
	cfg := makeCfgDespliegue(t,
		3,
		[]string{REPLICA1, REPLICA2, REPLICA3},
		[]bool{true, true, true})

	// tear down code
	// eliminar procesos en máquinas remotas
	defer cfg.stop()

	// Run test sequence

	// Test1 : No debería haber ningun primario, si SV no ha recibido aún latidos
	t.Run("T1:soloArranqueYparada",
		func(t *testing.T) { cfg.soloArranqueYparadaTest1(t) })

	// Test2 : No debería haber ningun primario, si SV no ha recibido aún latidos
	t.Run("T2:ElegirPrimerLider",
		func(t *testing.T) { cfg.elegirPrimerLiderTest2(t) })

	// Test3: tenemos el primer primario correcto
	t.Run("T3:FalloAnteriorElegirNuevoLider",
		func(t *testing.T) { cfg.falloAnteriorElegirNuevoLiderTest3(t) })

	// Test4: Tres operaciones comprometidas en configuración estable
	t.Run("T4:tresOperacionesComprometidasEstable",
		func(t *testing.T) { cfg.tresOperacionesComprometidasEstable(t) })

	t.Run("T5:AcuerdoAPesarDeDesconexionesDeSeguidor ",
		func(t *testing.T) { cfg.AcuerdoApesarDeSeguidor(t) })

	t.Run("T6:SinAcuerdoPorFallos ",
		func(t *testing.T) { cfg.SinAcuerdoPorFallos(t) })

	t.Run("T7:SometerConcurrentementeOperaciones ",
		func(t *testing.T) { cfg.SometerConcurrentementeOperaciones(t) })
}

// ---------------------------------------------------------------------
//
// Canal de resultados de ejecución de comandos ssh remotos
type canalResultados chan string

func (cr canalResultados) stop() {
	close(cr)

	// Leer las salidas obtenidos de los comandos ssh ejecutados
	for s := range cr {
		fmt.Println(s)
	}
}

// ---------------------------------------------------------------------
// Operativa en configuracion de despliegue y pruebas asociadas
type configDespliegue struct {
	t           *testing.T
	conectados  []bool
	numReplicas int
	nodosRaft   []rpctimeout.HostPort
	cr          canalResultados
}

// Crear una configuracion de despliegue
func makeCfgDespliegue(t *testing.T, n int, nodosraft []string,
	conectados []bool) *configDespliegue {
	cfg := &configDespliegue{}
	cfg.t = t
	cfg.conectados = conectados
	cfg.numReplicas = n
	cfg.nodosRaft = rpctimeout.StringArrayToHostPortArray(nodosraft)
	cfg.cr = make(canalResultados, 2000)
	return cfg
}

func (cfg *configDespliegue) stop() {
	//cfg.stopDistributedProcesses()

	time.Sleep(50 * time.Millisecond)

	cfg.cr.stop()
}

// --------------------------------------------------------------------------
// FUNCIONES DE SUBTESTS

// Se ponen en marcha replicas - 3 NODOS RAFT
func (cfg *configDespliegue) soloArranqueYparadaTest1(t *testing.T) {
	t.Skip("SKIPPED soloArranqueYparadaTest1")

	fmt.Println(t.Name(), ".....................")

	cfg.t = t // Actualizar la estructura de datos de tests para errores

	// Poner en marcha replicas en remoto con un tiempo de espera incluido
	cfg.startDistributedProcesses()

	// Comprobar estado replica 0
	cfg.comprobarEstadoRemoto(0)

	// Comprobar estado replica 1
	cfg.comprobarEstadoRemoto(1)

	// Comprobar estado replica 2
	cfg.comprobarEstadoRemoto(2)

	// Parar réplicas almacenamiento en remoto
	cfg.stopDistributedProcesses()

	fmt.Println(".............", t.Name(), "Superado")
}

// Primer lider en marcha - 3 NODOS RAFT
func (cfg *configDespliegue) elegirPrimerLiderTest2(t *testing.T) {
	t.Skip("SKIPPED ElegirPrimerLiderTest2")

	fmt.Println(t.Name(), ".....................")

	cfg.startDistributedProcesses()

	// Se ha elegido lider ?
	cfg.pruebaUnLider(3)

	// Parar réplicas alamcenamiento en remoto
	cfg.stopDistributedProcesses() // Parametros

	fmt.Println(".............", t.Name(), "Superado")
}

// Fallo de un primer lider y reeleccion de uno nuevo - 3 NODOS RAFT
func (cfg *configDespliegue) falloAnteriorElegirNuevoLiderTest3(t *testing.T) {
	t.Skip("SKIPPED FalloAnteriorElegirNuevoLiderTest3")

	fmt.Println(t.Name(), ".....................")
	cfg.startDistributedProcesses()

	// Get initial leader
	lider := cfg.pruebaUnLider(3)
	fmt.Printf("El leader es el nodo %d\n", lider)

	// Stop the leader
	fmt.Printf("Se para el nodo %d\n", lider)
	cfg.pararNodo(lider)

	// Wait for new leader election
	fmt.Printf("Esperando nuevo leader ...\n")
	time.Sleep(2000 * time.Millisecond)

	// Check new leader from next node in sequence
	var nuevoLider int
	_, _, _, nuevoLider, _ = cfg.obtenerEstadoRemoto((lider + 1) % 3)
	fmt.Printf("El nuevo leader es el nodo %d\n", nuevoLider)

	// Stop all nodes
	cfg.stopDistributedProcesses()

	fmt.Println(".............", t.Name(), "Superado")
}

// 3 operaciones comprometidas con situacion estable y sin fallos - 3 NODOS RAFT
func (cfg *configDespliegue) tresOperacionesComprometidasEstable(t *testing.T) {
	t.Skip("SKIPPED tresOperacionesComprometidasEstable")
	fmt.Println(t.Name(), ".....................")

	cfg.startDistributedProcesses()

	idLeader := cfg.pruebaUnLider(3)
	cfg.someterOperaciones(idLeader, 0, 3)
	cfg.comprobarEntradasComprometidas(idLeader, 3) // CHECK

	// Parar réplicas almacenamiento en remoto
	cfg.stopDistributedProcesses() //parametros
	fmt.Println(".............", t.Name(), "Superado")

}

// Se consigue acuerdo a pesar de desconexiones de seguidor -- 3 NODOS RAFT
func (cfg *configDespliegue) AcuerdoApesarDeSeguidor(t *testing.T) {
	t.Skip("SKIPPED AcuerdoApesarDeSeguidor")

	fmt.Println(t.Name(), ".....................")
	cfg.startDistributedProcesses()

	// explicación del uso del vector más abajo, donde se usa
	var nodes []int = []int{1, 2, 0}

	idLider := cfg.conseguirLider()

	cfg.someterOperaciones(idLider, 0, 1)

	cfg.comprobarEntradasComprometidas(idLider, 1) // CHECK

	cfg.desconectarNodos([]int{nodes[idLider]})

	cfg.someterOperaciones(idLider, 1, 3)

	cfg.reconectarNodos([]int{nodes[idLider]})

	time.Sleep(200 * time.Millisecond)
	cfg.comprobarEntradasComprometidas(idLider, 4) // CHECK
	// Parar réplicas almacenamiento en remoto
	cfg.stopDistributedProcesses() //parametros
	fmt.Println(".............", t.Name(), "Superado")
}

// NO se consigue acuerdo al desconectarse mayoría de seguidores -- 3 NODOS RAFT
func (cfg *configDespliegue) SinAcuerdoPorFallos(t *testing.T) {
	//t.Skip("SKIPPED SinAcuerdoPorFallos")
	fmt.Println(t.Name(), ".....................")

	cfg.startDistributedProcesses()

	// Get initial leader
	lider := cfg.conseguirLider()
	fmt.Printf("Líder inicial elegido: nodo %d\n", lider)

	// Submit first operation to verify cluster is working
	cfg.someterOperaciones(lider, 0, 1)
	cfg.comprobarEntradasComprometidas(lider, 1)

	// Disconnect both followers
	seguidor1 := (lider + 1) % 3
	seguidor2 := (lider + 2) % 3
	fmt.Printf("Desconectando seguidores: nodos %d y %d\n", seguidor1, seguidor2)
	cfg.desconectarNodos([]int{seguidor1, seguidor2})

	// Try to submit operations with disconnected majority
	fmt.Println("Intentando someter operaciones sin mayoría...")
	cfg.someterOperaciones(lider, 1, 3)

	// Get leader state
	_, _, _, _, indiceCommit := cfg.obtenerEstadoRemoto(lider)
	fmt.Printf("Índice commit del líder antes de reconexión: %d\n", indiceCommit)

	// Reconnect one follower
	fmt.Printf("Reconectando seguidor: nodo %d\n", seguidor1)
	cfg.reconectarNodos([]int{seguidor1})
	time.Sleep(2 * time.Second)

	// Verify operations are now committed
	cfg.comprobarEntradasComprometidas(lider, 4)

	// Reconnect second follower
	fmt.Printf("Reconectando seguidor: nodo %d\n", seguidor2)
	cfg.reconectarNodos([]int{seguidor2})
	time.Sleep(2 * time.Second)

	// Final verification
	for i := 0; i < 3; i++ {
		_, _, _, _, indice := cfg.obtenerEstadoRemoto(i)
		fmt.Printf("Nodo %d - Último índice comprometido: %d\n", i, indice)
	}

	cfg.stopDistributedProcesses()
	fmt.Println(".............", t.Name(), "Superado")
}

// Se somete 5 operaciones de forma concurrente -- 3 NODOS RAFT
func (cfg *configDespliegue) SometerConcurrentementeOperaciones(t *testing.T) {
	t.Skip("SKIPPED SometerConcurrentementeOperaciones")

	fmt.Println(t.Name(), ".....................")
	cfg.startDistributedProcesses()

	// Obtener un lider
	lider := cfg.conseguirLider()
	cfg.someterOperaciones(lider, 0, 1)

	index := make(chan int, 5)
	// Someter 5  operaciones concurrentemente
	for i := 0; i < 5; i++ {
		go func(i int) {
			operacion := raft.TipoOperacion{
				Operacion: "escribir",
				Clave:     fmt.Sprintf("key: %d", i),
				Valor:     fmt.Sprintf("value: %d", i),
			}

			var reply raft.ResultadoRemoto
			err := cfg.nodosRaft[lider].CallTimeout("NodoRaft.SometerOperacionRaft",
				operacion, &reply, 25*time.Millisecond)
			check.CheckError(err, "Error en llamada RPC SometerOperacionRaft")
			index <- reply.IndiceRegistro
		}(i)
	}
	time.Sleep(200 * time.Millisecond)
	var indexList map[int]int = make(map[int]int)
	for j := 0; j < 5; j++ {
		idx := <-index
		cfg.t.Log("Indice recibido: ", idx)
		indexList[idx] = idx
	}

	if len(indexList) != 5 {
		cfg.t.Fatalf("Estado incorrecto, no hay 5 indices distintos")
	}

	cfg.stopDistributedProcesses()
	fmt.Println(".............", t.Name(), "Superado")
}

// --------------------------------------------------------------------------
// FUNCIONES DE APOYO
// ----------

/*
func (cfg *configDespliegue) obtenerEstadoRemoto(
	indiceNodo int) (int, int, bool, int) {
	var reply raft.EstadoRemoto
	err := cfg.nodosRaft[indiceNodo].CallTimeout("NodoRaft.ObtenerEstadoNodo",
		raft.Vacio{}, &reply, 500*time.Millisecond)
	check.CheckError(err, "Error en llamada RPC ObtenerEstadoRemoto")

	return reply.IdNodo, reply.Mandato, reply.EsLider, reply.IdLider
}
*/

/*
var (
	nodosEnBarrera int
	barreraMutex   sync.Mutex
	barrierDone    chan struct{}
)
*/

// start  gestor de vistas; mapa de replicas y maquinas donde ubicarlos;
// y lista clientes (host:puerto)
func (cfg *configDespliegue) startDistributedProcesses() {
	/*
		if primeraInicializacion {
			barrierDone = make(chan struct{})
			nodosEnBarrera = 0
		}
	*/

	for i := len(cfg.nodosRaft) - 1; i >= 0; i-- {
		cfg.conectados[i] = true
		despliegue.ExecMutipleHosts(EXECREPLICACMD+
			" "+strconv.Itoa(i)+" "+
			rpctimeout.HostPortArrayToString(cfg.nodosRaft),
			[]string{cfg.nodosRaft[i].Host()}, cfg.cr)
	}

	time.Sleep(5 * time.Second)
	/*
		if primeraInicializacion {
			cfg.esperarBarrera()
			primeraInicializacion = false
		}
	*/
}

/*
func (cfg *configDespliegue) esperarBarrera() {
	fmt.Println("Esperando a que todos los nodos lleeguen a la barrera...")

	// Esperar a que todos los nodos reporten que están listos
	for i := 0; i < cfg.numReplicas; i++ {
		for {
			var reply raft.EstadoRemoto
			err := cfg.nodosRaft[i].CallTimeout("NodoRaft.ObtenerEstadoNodo",
				raft.Vacio{}, &reply, 500*time.Millisecond)
			if err == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		barreraMutex.Lock()
		nodosEnBarrera++
		fmt.Println("NIGA: ", nodosEnBarrera)
		if nodosEnBarrera == cfg.numReplicas {
			close(barrierDone)
		}
		barreraMutex.Unlock()
	}

	// Esperar a que la barrera se complete
	<-barrierDone
	fmt.Println("Todos los nodos han llegado a la barrera")
}
*/

func (cfg *configDespliegue) stopDistributedProcesses() {
	var reply raft.Vacio

	for i, endPoint := range cfg.nodosRaft {
		if cfg.conectados[i] {
			err := endPoint.CallTimeout("NodoRaft.ParaNodo",
				raft.Vacio{}, &reply, 10*time.Millisecond)
			check.CheckError(err, "Error en llamada RPC Para nodo")
			cfg.conectados[i] = false
		}
	}
}

// Comprobar estado remoto de un nodo con respecto a un estado prefijado
func (cfg *configDespliegue) comprobarEstadoRemoto(idNodoDeseado int) {
	idNodo, _, _, _, _ := cfg.obtenerEstadoRemoto(idNodoDeseado)

	//cfg.t.Log("Estado replica 0: ", idNodo, mandato, esLider, idLider, "\n")

	if idNodo != idNodoDeseado {
		cfg.t.Fatalf("Estado incorrecto en replica %d en subtest %s",
			idNodoDeseado, cfg.t.Name())
	}
}

func (cfg *configDespliegue) conseguirLider() int {
	var idLider int = -1
	for idLider == -1 {
		time.Sleep(25 * time.Millisecond)
		for i := 0; i < cfg.numReplicas; i++ {
			if cfg.conectados[i] {
				fmt.Println("soy el nodo", i)
				_, _, _, idLider, _ = cfg.obtenerEstadoRemoto(i) // aqui está el error
				fmt.Println("asdjflkadjfalkdfjakñldsjfaklsdfjl")
				if idLider != -1 {
					return idLider
				}
			}
		}
	}
	return idLider
}

// funcion para someter operaciones
func (cfg *configDespliegue) someterOperaciones(
	idLider, start, numOperaciones int) {
	for i := start; i < start+numOperaciones; i++ {
		operacion := raft.TipoOperacion{
			Operacion: "escribir",
			Clave:     fmt.Sprintf("key: %d", i),
			Valor:     fmt.Sprintf("value: %d", i),
		}

		// someter operación
		var reply raft.ResultadoRemoto
		err := cfg.nodosRaft[idLider].CallTimeout("NodoRaft.SometerOperacionRaft",
			operacion, &reply, 25*time.Millisecond)
		check.CheckError(err, "Error en llamada RPC SometerOperacionRaft")
	}
	time.Sleep(2000 * time.Millisecond)
}

func (cfg *configDespliegue) comprobarEntradasComprometidas(newLider, numAcuerdos int) {
	// Espero a que se consigan varios acuerdos
	time.Sleep(250 * time.Millisecond)

	// Obtener el estado de los almacenes de cada nodo
	var replies []raft.EstadoAlmacen = make([]raft.EstadoAlmacen, len(cfg.nodosRaft))
	for i := len(cfg.nodosRaft) - 1; i >= 0; i-- {
		endPoint := cfg.nodosRaft[i]
		fmt.Println("i: ", i)
		if cfg.conectados[i] {
			err := endPoint.CallTimeout("NodoRaft.ObtenerEstadoAlmacen",
				raft.Vacio{}, &replies[i], 25*time.Millisecond)

			// borrar
			fmt.Printf("Estado del almacén para el nodo %d:\n", i)
			fmt.Printf("  ID del Nodo: %d\n", replies[i].IdNodo)
			fmt.Println("  Log de Operaciones:")
			for _, entry := range replies[i].Log {
				fmt.Printf("    - Índice: %d, Mandato: %d, Operación: %s, Clave: %s, Valor: %s\n",
					entry.Indice, entry.Mandato, entry.Operacion.Operacion, entry.Operacion.Clave, entry.Operacion.Valor)
			}
			fmt.Println("  Almacen de Clave-Valor:")
			for key, value := range replies[i].Almacen {
				fmt.Printf("    - Clave: %s, Valor: %s\n", key, value)
			}

			check.CheckError(err, "Error en llamada RPC ObtenerEstadoAlmacen")
		}
	}

	if len(replies[newLider].Almacen) != numAcuerdos {
		cfg.t.Fatalf("No hay %d acuerdos", numAcuerdos)
	}

	// Comprobar que los acuerdos son iguales
	for i := 0; i < len(cfg.nodosRaft)-1; i++ {
		if cfg.conectados[i] {
			if len(replies[i].Almacen) != len(replies[i+1].Almacen) || len(replies[i].Log) != len(replies[i+1].Log) {
				cfg.t.Fatalf("No se han completado todos los acuerdos")
			}
		}
	}
}

func (cfg *configDespliegue) desconectarNodos(idNodos []int) {
	for _, nodo := range idNodos {
		var res raft.Vacio
		err := cfg.nodosRaft[nodo].CallTimeout("NodoRaft.ParaNodo",
			raft.Vacio{}, &res, 25*time.Millisecond)
		check.CheckError(err, "Error en llamada RPC Para nodo")
		cfg.conectados[nodo] = false
	}
}

func (cfg *configDespliegue) reconectarNodos(idNodos []int) {
	for _, nodo := range idNodos {
		if !cfg.conectados[nodo] {
			despliegue.ExecMutipleHosts(EXECREPLICACMD+
				" "+strconv.Itoa(nodo)+" "+
				rpctimeout.HostPortArrayToString(cfg.nodosRaft),
				[]string{cfg.nodosRaft[nodo].Host()}, cfg.cr)
			cfg.conectados[nodo] = true
		}
	}

	time.Sleep(5 * time.Second)
}

// Se para al nodo leader
func (cfg *configDespliegue) pararNodo(idNodo int) {
	var reply raft.Vacio
	for i, endPoint := range cfg.nodosRaft {
		if i == idNodo {
			err := endPoint.CallTimeout("NodoRaft.ParaNodo",
				raft.Vacio{}, &reply, 10*time.Millisecond)
			check.CheckError(err, "Error en llamada RPC Para nodo")
			cfg.conectados[i] = false
		}
	}
}

// Comprobar que hay un solo lider
// probar varias veces si se necesitan reelecciones
func (cfg *configDespliegue) pruebaUnLider(numreplicas int) int {
	for iters := 0; iters < 10; iters++ {
		time.Sleep(1000 * time.Millisecond)
		mapaLideres := make(map[int][]int)
		for i := 0; i < numreplicas; i++ {
			if cfg.conectados[i] {
				if _, mandato, eslider, _, _ := cfg.obtenerEstadoRemoto(i); eslider {
					mapaLideres[mandato] = append(mapaLideres[mandato], i)
				}
			}
		}

		ultimoMandatoConLider := -1
		for mandato, lideres := range mapaLideres {
			if len(lideres) > 1 {
				cfg.t.Fatalf("mandato %d tiene %d (>1) lideres",
					mandato, len(lideres))
			}
			if mandato > ultimoMandatoConLider {
				ultimoMandatoConLider = mandato
			}
		}

		if len(mapaLideres) != 0 {
			return mapaLideres[ultimoMandatoConLider][0] // Termina

		}
	}
	cfg.t.Fatalf("un lider esperado, ninguno obtenido")

	return -1 // Termina
}

func (cfg *configDespliegue) obtenerEstadoRemoto(
	indiceNodo int) (int, int, bool, int, error) {
	var reply raft.EstadoRemoto
	err := cfg.nodosRaft[indiceNodo].CallTimeout("NodoRaft.ObtenerEstadoNodo",
		raft.Vacio{}, &reply, 1000*time.Millisecond)

	if err != nil {
		fmt.Printf("Error en llamada RPC ObtenerEstadoRemoto: %v\n", err)
		return -1, -1, false, -1, err
	}

	return reply.IdNodo, reply.Mandato, reply.EsLider, reply.IdLider, nil
}

// Add this function if you need to start a single node
func (cfg *configDespliegue) startNodo(nodo int) {
	if !cfg.conectados[nodo] {
		despliegue.ExecMutipleHosts(EXECREPLICACMD+
			" "+strconv.Itoa(nodo)+" "+
			rpctimeout.HostPortArrayToString(cfg.nodosRaft),
			[]string{cfg.nodosRaft[nodo].Host()}, cfg.cr)
		cfg.conectados[nodo] = true
		time.Sleep(2 * time.Second)
	}
}

// Add this if you need to check committed index
func (cfg *configDespliegue) obtenerIndiceComprometidoRemoto(nodo int) (int, error) {
	var reply raft.EstadoRemoto
	err := cfg.nodosRaft[nodo].CallTimeout("NodoRaft.ObtenerEstadoNodo",
		raft.Vacio{}, &reply, 1000*time.Millisecond)
	if err != nil {
		return -1, err
	}
	return reply.IndiceCommit, nil
}
