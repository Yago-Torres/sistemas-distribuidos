package testintegracionraft1

import (
	"fmt"
	"raft/internal/comun/check"
	"raft/internal/comun/utils"

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
	//t.Skip("SKIPPED ElegirPrimerLiderTest2")

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
	//t.Skip("SKIPPED FalloAnteriorElegirNuevoLiderTest3")

	fmt.Println(t.Name(), ".....................")
	cfg.startDistributedProcesses()

	var nodes []int = []int{1, 2, 0}
	idLider := cfg.conseguirLider()
	cfg.desconectarNodos([]int{nodes[idLider]})

	for {
		_, _, _, idLider = cfg.obtenerEstadoRemoto(1)
		if idLider != -1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Parar réplicas almacenamiento en remoto
	cfg.stopDistributedProcesses() //parametros

	fmt.Println(".............", t.Name(), "Superado")
}

// 3 operaciones comprometidas con situacion estable y sin fallos - 3 NODOS RAFT
func (cfg *configDespliegue) tresOperacionesComprometidasEstable(t *testing.T) {
	// t.Skip("SKIPPED tresOperacionesComprometidasEstable")
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

	// Comprometer una entrada

	var nodes []int = []int{1, 2, 0}
	lider := cfg.conseguirLider()

	cfg.someterOperaciones(lider, 0, 1)

	cfg.comprobarEntradasComprometidas(lider, 1) // CHECK

	cfg.desconectarNodos([]int{nodes[lider], nodes[nodes[lider]]})

	cfg.someterOperaciones(lider, 1, 3)

	cfg.comprobarEntradasComprometidas(lider, 1, []int{nodes[lider],
		nodes[nodes[lider]]}) // CHECK

	fmt.Println("comprobarEntradasComprometidas")
	cfg.reconectarNodos([]int{nodes[lider], nodes[nodes[lider]]})
	fmt.Println("reconectarNodos")

	cfg.comprobarEntradasComprometidas(lider, 4) // CHECK
	fmt.Println("comprobarEntradasComprometidas")

	cfg.stopDistributedProcesses() //parametros
	fmt.Println("stopDistributedProcesses")

	fmt.Println(".............", t.Name(), "Superado")
}

// Se somete 5 operaciones de forma concurrente -- 3 NODOS RAFT
func (cfg *configDespliegue) SometerConcurrentementeOperaciones(t *testing.T) {
	//t.Skip("SKIPPED SometerConcurrentementeOperaciones")

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
// --------------------------------------------------------------------------

// Comprobar que hay un solo lider
// probar varias veces si se necesitan reelecciones
func (cfg *configDespliegue) pruebaUnLider(numreplicas int) int {
	for iters := 0; iters < 10; iters++ {
		time.Sleep(500 * time.Millisecond)
		mapaLideres := make(map[int][]int)
		for i := 0; i < numreplicas; i++ {
			if cfg.conectados[i] {
				if _, mandato, eslider, _ := cfg.obtenerEstadoRemoto(i); eslider {
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
	indiceNodo int) (int, int, bool, int) {
	var reply raft.EstadoRemoto
	err := cfg.nodosRaft[indiceNodo].CallTimeout("NodoRaft.ObtenerEstadoNodo",
		raft.Vacio{}, &reply, 500*time.Millisecond)
	check.CheckError(err, "Error en llamada RPC ObtenerEstadoRemoto")

	return reply.IdNodo, reply.Mandato, reply.EsLider, reply.IdLider
}

// start  gestor de vistas; mapa de replicas y maquinas donde ubicarlos;
// y lista clientes (host:puerto)
func (cfg *configDespliegue) startDistributedProcesses() {
	//cfg.t.Log("Before start following distributed processes: ", cfg.nodosRaft)
	for i, endPoint := range cfg.nodosRaft {
		despliegue.ExecMutipleHosts(EXECREPLICACMD+
			" "+strconv.Itoa(i)+" "+
			rpctimeout.HostPortArrayToString(cfg.nodosRaft),
			[]string{endPoint.Host()}, cfg.cr)

		// dar tiempo para se establezcan las replicas
		//time.Sleep(300 * time.Millisecond)
	}

	// aproximadamente 500 ms para cada arranque por ssh en portatil
	time.Sleep(12 * time.Second)
}

func (cfg *configDespliegue) stopDistributedProcesses() {
	var reply raft.Vacio

	for i, endPoint := range cfg.nodosRaft {
		if cfg.conectados[i] {
			err := endPoint.CallTimeout("NodoRaft.ParaNodo",
				raft.Vacio{}, &reply, 10*time.Millisecond)
			check.CheckError(err, "Error en llamada RPC Para nodo")
		}
	}
}

// Comprobar estado remoto de un nodo con respecto a un estado prefijado
func (cfg *configDespliegue) comprobarEstadoRemoto(idNodoDeseado int) {
	idNodo, _, _, _ := cfg.obtenerEstadoRemoto(idNodoDeseado)

	//cfg.t.Log("Estado replica 0: ", idNodo, mandato, esLider, idLider, "\n")

	if idNodo != idNodoDeseado {
		cfg.t.Fatalf("Estado incorrecto en replica %d en subtest %s",
			idNodoDeseado, cfg.t.Name())
	}
}

func (cfg *configDespliegue) conseguirLider() int {
	var idLider int
	for {
		time.Sleep(20 * time.Millisecond)
		_, _, _, idLider = cfg.obtenerEstadoRemoto(0)
		if idLider != -1 {
			break
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

func (cfg *configDespliegue) comprobarEntradasComprometidas(idLider, numAcuerdos int, caidos ...[]int) {
	// Espero a que se consigan varios acuerdos
	time.Sleep(250 * time.Millisecond)

	// Obtener el estado de los almacenes de cada nodo
	var replies []raft.EstadoAlmacen = make([]raft.EstadoAlmacen, len(cfg.nodosRaft))
	for i, endPoint := range cfg.nodosRaft {
		fmt.Println("i: ", i)
		if caidos == nil {
			err := endPoint.CallTimeout("NodoRaft.ObtenerEstadoAlmacen",
				raft.Vacio{}, &replies[i], 25*time.Millisecond)
			fmt.Println("replies[i]: ", replies[i])
			check.CheckError(err, "Error en llamada RPC ObtenerEstadoAlmacen")
		} else if !utils.EstaEnLista(i, caidos[0]) {
			err := endPoint.CallTimeout("NodoRaft.ObtenerEstadoAlmacen",
				raft.Vacio{}, &replies[i], 25*time.Millisecond)
			check.CheckError(err, "Error en llamada RPC ObtenerEstadoAlmacen")
		}
	}

	// Busco el indice del registro en el lider
	var idxLider int
	for _, reply := range replies {
		if reply.IdNodo == idLider {
			idxLider = reply.IdNodo
			break
		}
	}

	// Comprobar que el numero de acuerdos es el esperado
	if len(replies[idxLider].Almacen) != numAcuerdos {
		cfg.t.Fatalf("No hay %d acuerdos", numAcuerdos)
	}

	// Comprobar que los acuerdos son iguales
	for i := 0; i < len(cfg.nodosRaft)-1; i++ {
		if len(replies[i].Almacen) != len(replies[i+1].Almacen) ||
			len(replies[i].Log) != len(replies[i+1].Log) {
			cfg.t.Fatalf("No se han completado todos los acuerdos")
		}
	}
}

func (cfg *configDespliegue) desconectarNodos(idNodos []int) {
	for _, nodo := range idNodos {
		var res raft.Vacio
		err := cfg.nodosRaft[nodo].CallTimeout("NodoRaft.ParaNodo",
			raft.Vacio{}, &res, 25*time.Millisecond)
		check.CheckError(err, "Error en llamada RPC Para nodo")
	}
}

func (cfg *configDespliegue) reconectarNodos(idNodos []int) {
	for _, nodo := range idNodos {
		despliegue.ExecMutipleHosts(EXECREPLICACMD+" "+strconv.Itoa(nodo)+" "+
			rpctimeout.HostPortArrayToString(cfg.nodosRaft),
			[]string{cfg.nodosRaft[nodo].Host()}, cfg.cr)
	}
	time.Sleep(2500 * time.Millisecond)
}
