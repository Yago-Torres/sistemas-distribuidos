package testintegracionraft1

import (
	"fmt"
	"raft/internal/comun/check"
	"sync"

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
	//REPLICA1 = "192.168.3.1:29254"
	//REPLICA2 = "192.168.3.2:29254"
	//REPLICA3 = "192.168.3.3:29254"
	// paquete main de ejecutables relativos a directorio raiz de modulo
	EXECREPLICA = "cmd/srvraft/main.go"

	// comando completo a ejecutar en máquinas remota con ssh. Ejemplo :
	// 				cd $HOME/raft; go run cmd/srvraft/main.go 127.0.0.1:29001
)

// PATH de los ejecutables de modulo golang de servicio Raft
var cwd, _ = os.Getwd()
var PATH string = filepath.Dir(filepath.Dir(cwd))

// go run cmd/srvraft/main.go 0 127.0.0.1:29001 127.0.0.1:29002 127.0.0.1:29003
var EXECREPLICACMD string = "cd " + PATH + "; /usr/bin/go run " + EXECREPLICA

//////////////////////////////////////////////////////////////////////////////
///////////////////////			 FUNCIONES TEST
/////////////////////////////////////////////////////////////////////////////

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

	// Test5: Se consigue acuerdo a pesar de desconexiones de seguidor
	t.Run("T5:AcuerdoAPesarDeDesconexionesDeSeguidor ",
		func(t *testing.T) { cfg.AcuerdoApesarDeSeguidor(t) })

	t.Run("T6:SinAcuerdoPorFallos ",
		func(t *testing.T) { cfg.SinAcuerdoPorFallos(t) })

	t.Run("T7:SometerConcurrentementeOperaciones ",
		func(t *testing.T) { cfg.SometerConcurrentementeOperaciones(t) })
}

/*
// TEST primer rango
func TestAcuerdosConFallos(t *testing.T) { // (m *testing.M) {
	// <setup code>
	// Crear canal de resultados de ejecuciones ssh en maquinas remotas
	cfg := makeCfgDespliegue(t,
		3,
		[]string{REPLICA1, REPLICA2, REPLICA3},
		[]bool{true, true, true})

	// tear down code
	// eliminar procesos en máquinas remotas
	defer cfg.stop()

	// Test5: Se consigue acuerdo a pesar de desconexiones de seguidor
	t.Run("T5:AcuerdoAPesarDeDesconexionesDeSeguidor ",
		func(t *testing.T) { cfg.AcuerdoApesarDeSeguidor(t) })

	t.Run("T6:SinAcuerdoPorFallos ",
		func(t *testing.T) { cfg.SinAcuerdoPorFallos(t) })

	t.Run("T7:SometerConcurrentementeOperaciones ",
		func(t *testing.T) { cfg.SometerConcurrentementeOperaciones(t) })

}
*/
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
	//t.Skip("SKIPPED soloArranqueYparadaTest1")

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
	//fmt.Printf("Probando lider en curso\n")
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

	leader := cfg.pruebaUnLider(3)
	//fmt.Printf("El leader es el nodo %d\n", leader)
	// Parar el lider
	//fmt.Printf("Se para el nodo %d\n", leader)
	cfg.pararLeader(leader)
	// 	Se comprueba un nuevo líder
	//fmt.Printf("Esperenado nuevo leader ...\n")
	//Esperar a que se actualize el lider

	//Esperar a que se actualize el lider
	time.Sleep(2000 * time.Millisecond)
	_, _, _, _, _ = cfg.obtenerEstadoRemoto((leader + 1) % 3)

	// Parar réplicas almacenamiento en remoto
	cfg.stopDistributedProcesses() //parametros

	fmt.Println(".............", t.Name(), "Superado")
}

// 3 operaciones comprometidas con situacion estable y sin fallos - 3 NODOS RAFT
func (cfg *configDespliegue) tresOperacionesComprometidasEstable(t *testing.T) {
	//t.Skip("SKIPPED TresOperacionesComprometidasEstable")

	fmt.Println(t.Name(), ".....................")

	cfg.startDistributedProcesses()
	idLeader := cfg.pruebaUnLider(3)

	// Write a value first
	cfg.comprobarOperacion(idLeader, 0, "escribir", "clave1", "hola mundo")

	// Read the value we just wrote
	cfg.comprobarOperacion(idLeader, 1, "leer", "clave1", "")

	// Write another value
	cfg.comprobarOperacion(idLeader, 2, "escribir", "clave2", "segundo valor")

	// Parar réplicas almacenamiento en remoto
	cfg.stopDistributedProcesses()
	fmt.Println(".............", t.Name(), "Superado")
}

// Se consigue acuerdo a pesar de desconexiones de seguidor -- 3 NODOS RAFT
// A completar ???

// Comprometer una entrada

//  Obtener un lider y, a continuación desconectar una de los nodos Raft

// Comprobar varios acuerdos con una réplica desconectada

// reconectar nodo Raft previamente desconectado y comprobar varios acuerdos

// Se consigue acuerdo a pesar de desconexiones de seguidor -- 3 NODOS RAFT
func (cfg *configDespliegue) AcuerdoApesarDeSeguidor(t *testing.T) {
	//t.Skip("SKIPPED AcuerdoApesarDeSeguidor")
	fmt.Println("Iniciando Test 5: AcuerdoAPesarDeDesconexionesDeSeguidor")

	// Iniciar los procesos distribuidos para los nodos Raft
	cfg.startDistributedProcesses()
	time.Sleep(3 * time.Second)
	// Paso 1: Obtener un líder
	leader := cfg.pruebaUnLider(3)
	//fmt.Printf("Líder inicial elegido: nodo %d\n", leader)

	// Paso 2: Desconectar uno de los seguidores
	seguidorDesconectado := (leader + 1) % 3 // Elegimos un nodo que no sea el líder
	//fmt.Printf("Desconectando el seguidor: nodo %d\n", seguidorDesconectado)
	cfg.pararLeader(seguidorDesconectado) // Desconectar seguidor

	// Paso 3: Verificar que el líder sigue funcionando
	//fmt.Println("Sometiendo operaciones mientras un seguidor está desconectado")
	cfg.comprobarOperacion(leader, 0, "escribir", "clave54", "valor1")
	cfg.comprobarOperacion(leader, 1, "leer", "clave1", "")
	cfg.comprobarOperacion(leader, 2, "escribir", "clave2", "valor2")

	// Paso 4: Reconectar el seguidor desconectado
	//fmt.Printf("Reconectando el seguidor: nodo %d\n", seguidorDesconectado)
	cfg.startNodo(seguidorDesconectado)

	// Dar tiempo para que el nodo reconectado se sincronice con el líder
	time.Sleep(5000 * time.Millisecond)

	// Paso 5: Verificar que todos los nodos están sincronizados en el líder y en el último índice comprometido
	// Obtener el índice comprometido del líder mediante la llamada RPC
	commitIndexLeader, err := cfg.obtenerIndiceComprometidoRemoto(leader)
	if err != nil {
		cfg.t.Fatalf("Error al obtener índice comprometido del líder: %v", err)
	}

	for i := 0; i < 3; i++ {
		// Obtener el índice comprometido de cada nodo mediante la llamada RPC
		commitIndex, err := cfg.obtenerIndiceComprometidoRemoto(i)
		if err != nil {
			cfg.t.Fatalf("Error al obtener índice comprometido del nodo %d: %v", i, err)
		}

		// Obtener el líder percibido en el nodo i
		_, _, _, idLider, _ := cfg.obtenerEstadoRemoto(i)
		//fmt.Printf("Nodo %d - Último índice comprometido: %d, Líder: %d\n", i, commitIndex, idLider)

		// Comprobar que el líder percibido es el mismo
		if idLider != leader {
			cfg.t.Fatalf("El líder actual no coincide después de la reconexión en nodo %d, esperado: %d, obtenido: %d", i, leader, idLider)
		}

		// Comprobar que el índice comprometido coincide con el del líder
		if commitIndex != commitIndexLeader {
			cfg.t.Fatalf("El índice comprometido no coincide en nodo %d, esperado: %d, obtenido: %d", i, commitIndexLeader, commitIndex)
		}
	}

	fmt.Println("Test 5: AcuerdoAPesarDeDesconexionesDeSeguidor superado")
	cfg.stopDistributedProcesses()
}

// NO se consigue acuerdo al desconectarse mayoría de seguidores -- 3 NODOS RAFT

// A completar ???

// Comprometer una entrada

//  Obtener un lider y, a continuación desconectar 2 de los nodos Raft

// Comprobar varios acuerdos con 2 réplicas desconectada

// reconectar lo2 nodos Raft  desconectados y probar varios acuerdos
func (cfg *configDespliegue) SinAcuerdoPorFallos(t *testing.T) {
	//t.Skip("SKIPPED SinAcuerdoPorFallos")
	fmt.Println("Iniciando Test: Acuerdo con dos seguidores desconectados")

	// Iniciar los procesos distribuidos para los nodos Raft
	cfg.startDistributedProcesses()

	// Paso 1: Obtener un líder
	leader := cfg.pruebaUnLider(3)
	//fmt.Printf("Líder inicial elegido: nodo %d\n", leader)

	// Paso 2: Desconectar ambos seguidores
	seguidor1 := (leader + 1) % 3
	seguidor2 := (leader + 2) % 3
	//fmt.Printf("Desconectando seguidores: nodos %d y %d\n", seguidor1, seguidor2)
	cfg.pararLeader(seguidor1)
	cfg.pararLeader(seguidor2)

	// Paso 3: Intentar someter operaciones mientras los seguidores están desconectados
	//fmt.Println("Sometiendo operaciones mientras ambos seguidores están desconectados")
	cfg.comprobarOperacion(leader, 0, "escribir", "clave1", "valor1")
	cfg.comprobarOperacion(leader, 1, "escribir", "clave2", "valor2")
	cfg.comprobarOperacion(leader, 2, "escribir", "clave3", "valor3")

	// Verificar que el líder no ha comprometido las operaciones aún
	leaderCommit, _ := cfg.obtenerIndiceComprometidoRemoto(leader)
	//fmt.Printf("Antes de la reconexión: LeaderCommit del líder: %d\n", leaderCommit)
	if leaderCommit != -1 {
		t.Fatalf("El líder no debería haber comprometido ninguna entrada todavía")
	}

	// Paso 4: Reconectar uno de los seguidores
	//fmt.Printf("Reconectando seguidor: nodo %d\n", seguidor1)
	cfg.startNodo(seguidor1)

	// Dar tiempo para que se alcance el consenso
	time.Sleep(2000 * time.Millisecond)

	// Verificar que el líder ha comprometido las operaciones
	leaderCommit, _ = cfg.obtenerIndiceComprometidoRemoto(leader)
	//fmt.Printf("Después de reconectar un seguidor: LeaderCommit del líder: %d\n", leaderCommit)
	if leaderCommit != 2 {
		t.Fatalf("El líder debería haber comprometido las 3 entradas")
	}

	// Paso 5: Reconectar el segundo seguidor y verificar la sincronización
	//fmt.Printf("Reconectando seguidor: nodo %d\n", seguidor2)
	cfg.startNodo(seguidor2)
	time.Sleep(2000 * time.Millisecond)

	// Verificar que todos los nodos están sincronizados
	for i := 0; i < 3; i++ {
		commitIndex, _ := cfg.obtenerIndiceComprometidoRemoto(i)
		//fmt.Printf("Nodo %d - Último índice comprometido: %d\n", i, commitIndex)
		if commitIndex != 2 {
			t.Fatalf("El nodo %d no está sincronizado correctamente", i)
		}
	}

	fmt.Println("Test: Acuerdo con dos seguidores desconectados superado")
	cfg.stopDistributedProcesses()
}

// Se somete 5 operaciones de forma concurrente -- 3 NODOS RAFT
// A completar ???

// un bucle para estabilizar la ejecucion

// Obtener un lider y, a continuación someter una operacion

// Someter 5  operaciones concurrentes

// Comprobar estados de nodos Raft, sobre todo
// el avance del mandato en curso e indice de registro de cada uno
// que debe ser identico entre ellos

func (cfg *configDespliegue) SometerConcurrentementeOperaciones(t *testing.T) {
	//t.Skip("SKIPPED SometerConcurrentementeOperaciones")

	fmt.Println("Iniciando Test: Someter Concurrentemente Operaciones")

	// Paso 1: Iniciar los procesos distribuidos para los nodos Raft
	cfg.startDistributedProcesses()

	// Paso 2: Obtener un líder
	leader := cfg.pruebaUnLider(3)
	//fmt.Printf("Líder inicial elegido: nodo %d\n", leader)

	// Paso 3: Crear un WaitGroup para esperar a que todas las gorutinas terminen
	var wg sync.WaitGroup
	numOperaciones := 5
	confirmaciones := make(chan int, numOperaciones)

	// Paso 4: Someter 5 operaciones concurrentes al líder usando gorutinas
	for i := 0; i < numOperaciones; i++ {
		wg.Add(1)
		go func(operacionID int) {
			defer wg.Done()

			// Generar una clave y un valor para la operación
			clave := fmt.Sprintf("clave%d", operacionID)
			valor := fmt.Sprintf("valor%d", operacionID)

			// Someter la operación al líder
			//fmt.Printf("Sometiendo operación %d al líder %d: escribir %s = %s\n", operacionID, leader, clave, valor)
			indice, _, _, idLider, _ := cfg.someterOperacion(leader, "escribir", clave, valor)

			// Comprobar si la operación fue sometida correctamente
			if indice != operacionID || idLider != leader {
				return
			}
			// Si fue exitosa, enviar confirmación al canal
			confirmaciones <- indice
		}(i)
	}

	// Paso 5: Esperar a que todas las operaciones se sometan
	wg.Wait()
	close(confirmaciones)

	time.Sleep(2 * time.Second)
	// Paso 6: Verificar que se han comprometido todas las operaciones
	//fmt.Println("Verificando estados de los nodos...")
	//for commitIndex := range confirmaciones {
	//fmt.Printf("Operación confirmada con índice: %d\n", commitIndex)
	//}

	// Paso 7: Verificar que todos los nodos tienen el mismo estado
	for nodo := 0; nodo < 3; nodo++ {
		commitIndex, err := cfg.obtenerIndiceComprometidoRemoto(nodo)
		if err != nil {
			t.Fatalf("Error al obtener índice comprometido del nodo %d: %v", nodo, err)
		}
		//fmt.Printf("Nodo %d - Último índice comprometido: %d\n", nodo, commitIndex)

		// Comprobamos que el índice comprometido sea igual al número total de operaciones - 1
		if commitIndex != numOperaciones-1 {
			t.Fatalf("El nodo %d no está sincronizado correctamente. Se esperaba commitIndex = %d, obtenido = %d", nodo, numOperaciones-1, commitIndex)
		}
	}

	fmt.Println("Test: Someter Concurrentemente Operaciones superado")
	cfg.stopDistributedProcesses()
}

// --------------------------------------------------------------------------
// FUNCIONES DE APOYO
// --------------------------------------------------------------------------

func (cfg *configDespliegue) someterOperacion(idLeader int, operation string,
	clave string, valor string) (int, int, bool, int, string) {
	operacion := raft.TipoOperacion{

		Operacion: operation,
		Clave:     clave,
		Valor:     valor,
	}
	var reply raft.ResultadoRemoto
	err := cfg.nodosRaft[idLeader].CallTimeout("NodoRaft.SometerOperacionRaft",
		operacion, &reply, 2000*time.Millisecond)
	// Manejo de error en la llamada RPC
	if err != nil {
		check.CheckError(err, "Error en llamada RPC SometerOperacionRaft")
	}

	// Devuelve el resultado de la operación
	return reply.IndiceRegistro, reply.Mandato, reply.EsLider, reply.IdLider, reply.ValorADevolver
}

func (cfg *configDespliegue) comprobarOperacion(idLeader int, index int,
	operation string, clave string, valor string) {
	// Se somete la operación al nodo Raft
	indice, _, _, idLider, _ := cfg.someterOperacion(idLeader, operation, clave, valor)

	// Verifica si el índice de la operación sometida es igual al índice esperado
	if indice != index || idLider != idLeader {
		cfg.t.Fatalf("No se ha soemetido correctamente la operación con índice %d, se obtuvo índice %d", index, indice)
		//println("error")
	}
}

// Se para al nodo leader
func (cfg *configDespliegue) pararLeader(idLeader int) {
	var reply raft.Vacio
	endPoint := cfg.nodosRaft[idLeader]
	err := endPoint.CallTimeout("NodoRaft.ParaNodo",
		raft.Vacio{}, &reply, 500*time.Millisecond)
	check.CheckError(err, "Error en llamada RPC Para nodo")
	cfg.conectados[idLeader] = false
}

// Comprobar que hay un solo lider
// probar varias veces si se necesitan reelecciones
func (cfg *configDespliegue) pruebaUnLider(numreplicas int) int {
	for iters := 0; iters < 10; iters++ {
		time.Sleep(1500 * time.Millisecond)
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
		raft.Vacio{}, &reply, 500*time.Millisecond)

	if err != nil {
		fmt.Printf("Error en llamada RPC ObtenerEstadoRemoto: %v\n", err)
		return -1, -1, false, -1, err
	}

	return reply.IdNodo, reply.Mandato, reply.EsLider, reply.IdLider, nil
}

// Esta función realizará la llamada RPC al método ObtenerIndiceComprometido de NodoRaft y devolverá el commitIndex de un nodo remoto.
func (cfg *configDespliegue) obtenerIndiceComprometidoRemoto(indiceNodo int) (int, error) {
	var reply raft.RespuestaIndiceComprometido
	err := cfg.nodosRaft[indiceNodo].CallTimeout("NodoRaft.ObtenerIndiceComprometido",
		raft.Vacio{}, &reply, 500*time.Millisecond)

	if err != nil {
		return -1, err
	}

	return reply.CommitIndex, nil
}

// start  gestor de vistas; mapa de replicas y maquinas donde ubicarlos;
// y lista clientes (host:puerto)
func (cfg *configDespliegue) startDistributedProcesses() {
	//cfg.t.Log("Before start following distributed processes: ", cfg.nodosRaft)
	var wg sync.WaitGroup
	for i, endPoint := range cfg.nodosRaft {
		wg.Add(1)
		go func(i int, endPoint *rpctimeout.HostPort) {
			defer wg.Done()
			despliegue.ExecMutipleHosts(EXECREPLICACMD+
				" "+strconv.Itoa(i)+" "+
				rpctimeout.HostPortArrayToString(cfg.nodosRaft),
				[]string{endPoint.Host()}, cfg.cr)

			// dar tiempo para se establezcan las replicas
			cfg.conectados[i] = true // Make sure this is set

			time.Sleep(1500 * time.Millisecond)
		}(i, &endPoint)
	}
	wg.Wait()

	cfg.waitForNodesReady() // vamos a usar algo "similar" a un barrier
	// será una espera activa para los nodos, hasta que todos estén disponibles
}

func (cfg *configDespliegue) waitForNodesReady() {
	timeout := time.After(20 * time.Second) // Tiempo máximo de espera, según pruebas, no tardan más de 10 segundos en ninguno de los casos
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			cfg.t.Fatalf("Timeout esperando que todos los nodos estén listos")
		case <-ticker.C:
			allReady := true
			for i := 0; i < cfg.numReplicas; i++ {
				if cfg.conectados[i] {
					_, _, _, _, err := cfg.obtenerEstadoRemoto(i)
					if err != nil {
						allReady = false
						break
					}
				}
			}
			if allReady {
				return
			}
		}
	}
}

func (cfg *configDespliegue) stopDistributedProcesses() {
	var reply raft.Vacio

	for i, endPoint := range cfg.nodosRaft {
		if cfg.conectados[i] {
			err := endPoint.CallTimeout("NodoRaft.ParaNodo",
				raft.Vacio{}, &reply, 500*time.Millisecond)
			check.CheckError(err, "Error en llamada RPC Para nodo")
		}
		time.Sleep(2000 * time.Millisecond)
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

// startNodo arranca un nodo Raft específico
func (cfg *configDespliegue) startNodo(nodo int) {
	if nodo < 0 || nodo >= len(cfg.nodosRaft) {
		cfg.t.Fatalf("Índice de nodo fuera de rango: %d", nodo)
	}

	endPoint := cfg.nodosRaft[nodo]
	cfg.t.Logf("Arrancando nodo %d en %s", nodo, endPoint.Host())

	// Ejecutar el comando para arrancar el nodo específico
	despliegue.ExecMutipleHosts(
		EXECREPLICACMD+" "+strconv.Itoa(nodo)+" "+rpctimeout.HostPortArrayToString(cfg.nodosRaft),
		[]string{endPoint.Host()}, cfg.cr)

	// Dar tiempo para que el nodo se establezca
	time.Sleep(5000 * time.Millisecond)

	// Comprobación de que el nodo está operativo
	var estadoRemoto raft.EstadoRemoto
	err := endPoint.CallTimeout("NodoRaft.ObtenerEstadoNodo", raft.Vacio{}, &estadoRemoto, 500*time.Millisecond)

	if err != nil {
		cfg.t.Fatalf("Error al verificar el estado del nodo %d: %v", nodo, err)
	} else {
		cfg.t.Logf("Nodo %d arrancado correctamente. Estado: Nodo: %d, Mandato: %d, EsLider: %v, IdLider: %d",
			nodo, estadoRemoto.IdNodo, estadoRemoto.Mandato, estadoRemoto.EsLider, estadoRemoto.IdLider)
		cfg.conectados[nodo] = true
	}
}
