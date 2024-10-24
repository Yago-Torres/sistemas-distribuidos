// Escribir vuestro código de funcionalidad Raft en este fichero
//

package raft

//
// API
// ===
// Este es el API que vuestra implementación debe exportar
//
// nodoRaft = NuevoNodo(...)
//   Crear un nuevo servidor del grupo de elección.
//
// nodoRaft.Para()
//   Solicitar la parado de un servidor
//
// nodo.ObtenerEstado() (yo, mandato, esLider)
//   Solicitar a un nodo de elección por "yo", su mandato en curso,
//   y si piensa que es el msmo el lider
//
// nodoRaft.SometerOperacion(operacion interface()) (indice, mandato, esLider)

// type AplicaOperacion

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"

	//"crypto/rand"
	"sync"
	"time"

	//"net/rpc"

	"raft/internal/comun/rpctimeout"
)

const (
	// Constante para fijar valor entero no inicializado
	IntNOINICIALIZADO = -1

	//  false deshabilita por completo los logs de depuracion
	// Aseguraros de poner kEnableDebugLogs a false antes de la entrega
	kEnableDebugLogs = true

	// Poner a true para logear a stdout en lugar de a fichero
	kLogToStdout = false

	// Cambiar esto para salida de logs en un directorio diferente
	kLogOutputDir = "./logs_raft/"
)

type TipoOperacion struct {
	Operacion string // La operaciones posibles son "leer" y "escribir"
	Clave     string
	Valor     string // en el caso de la lectura Valor = ""
}

// A medida que el nodo Raft conoce las operaciones de las  entradas de registro
// comprometidas, envía un AplicaOperacion, con cada una de ellas, al canal
// "canalAplicar" (funcion NuevoNodo) de la maquina de estados
type AplicaOperacion struct {
	Indice    int // en la entrada de registro
	Operacion TipoOperacion
}

// tipo para guardar operaciones
type EntradaLog struct {
	Mandato   int
	Operacion TipoOperacion
}

// Tipo de dato Go que representa un solo nodo (réplica) de raft
type NodoRaft struct {
	Mux sync.Mutex // Mutex para proteger acceso a estado compartido

	// Host:Port de todos los nodos (réplicas) Raft, en mismo orden
	Nodos   []rpctimeout.HostPort
	Yo      int // indice de este nodos en campo array "nodos"
	IdLider int
	// Utilización opcional de este logger para depuración
	// Cada nodo Raft tiene su propio registro de trazas (logs)
	Logger *log.Logger

	// Vuestros datos aqui.

	Mandato             int
	UltimoMandatoLogged int
	UltimoIndiceLogged  int

	log                []EntradaLog // vector de tiposoperación que se han hecho
	indiceComprometido int
	recibirLatido      chan struct{} // canal para recepción de latidos

	matchIndex []int // Índice más alto replicado por cada nodo
	nextIndex  []int // Siguiente índice a enviar para cada nodo

}

// Creacion de un nuevo nodo de eleccion
//
// Tabla de <Direccion IP:puerto> de cada nodo incluido a si mismo.
//
// <Direccion IP:puerto> de este nodo esta en nodos[yo]
//
// Todos los arrays nodos[] de los nodos tienen el mismo orden

// canalAplicar es un canal donde, en la practica 5, se recogerán las
// operaciones a aplicar a la máquina de estados. Se puede asumir que
// este canal se consumira de forma continúa.
//
// NuevoNodo() debe devolver resultado rápido, por lo que se deberían
// poner en marcha Gorutinas para trabajos de larga duracion
func NuevoNodo(nodos []rpctimeout.HostPort, yo int,
	canalAplicarOperacion chan AplicaOperacion) *NodoRaft {
	nr := &NodoRaft{}
	nr.Nodos = nodos
	nr.Yo = yo
	nr.IdLider = -1
	nr.Mandato = 0                             // Por iniciar el mandato a un valor excepción, que comience en 1
	nr.recibirLatido = make(chan struct{})     // Inicializamos el canal de latidos
	nr.matchIndex = make([]int, len(nr.Nodos)) // Inicializa matchIndex
	nr.nextIndex = make([]int, len(nr.Nodos))  // Inicializa nextIndex
	nr.indiceComprometido = 0                  // Inicializa el índice comprometido
	nr.log = make([]EntradaLog, 0)             // Inicializa el log vacío

	// A partir de aquí, nr está inicializado con mis datos + lider-1

	if kEnableDebugLogs {

		nombreNodo := nodos[yo].Host() + "_" + nodos[yo].Port()

		if kLogToStdout {
			nr.Logger = log.New(os.Stdout, nombreNodo+" -->> ",
				log.Lmicroseconds|log.Lshortfile)
		} else {
			err := os.MkdirAll(kLogOutputDir, os.ModePerm)
			if err != nil {
				panic(err.Error())
			}
			logOutputFile, err := os.OpenFile(
				fmt.Sprintf("%s/%s.txt", kLogOutputDir, nombreNodo),
				os.O_RDWR|os.O_CREATE|os.O_TRUNC,
				0755)
			if err != nil {
				panic(err.Error())
			}
			nr.Logger = log.New(logOutputFile,
				nombreNodo+" -> ", log.Lmicroseconds|log.Lshortfile)
		}
		nr.Logger.Println("logger initialized")
	} else {
		nr.Logger = log.New(io.Discard, "", 0)
	}

	// Llama a la función para iniciar el control de timeout y elecciones
	nr.iniciarTimeoutControl()

	// Añadir codigo de inicialización adicional si es necesario

	return nr
}

// Metodo Para() utilizado cuando no se necesita mas al nodo
//
// Quizas interesante desactivar la salida de depuracion
// de este nodo
func (nr *NodoRaft) para() {
	go func() { time.Sleep(50 * time.Millisecond); os.Exit(0) }()
}

// Devuelve "yo", mandato en curso y si este nodo cree ser lider
//
// Primer valor devuelto es el indice de este  nodo Raft el el conjunto de nodos
// la operacion si consigue comprometerse.
// El segundo valor es el mandato en curso
// El tercer valor es true si el nodo cree ser el lider
// Cuarto valor es el lider, es el indice del líder si no es él
func (nr *NodoRaft) obtenerEstado() (int, int, bool, int) {
	var yo int = nr.Yo
	var mandato int = nr.Mandato
	var esLider bool = (nr.Yo == nr.IdLider)
	var idLider int = nr.IdLider

	// Vuestro codigo aqui
	fmt.Printf("devuelvo estado: Yo: %d, Mandato: %d, EsLider: %t, IdLider: %d\n", yo, mandato, esLider, idLider)
	return yo, mandato, esLider, idLider
}

// El servicio que utilice Raft (base de datos clave/valor, por ejemplo)
// Quiere buscar un acuerdo de posicion en registro para siguiente operacion
// solicitada por cliente.

// Si el nodo no es el lider, devolver falso
// Sino, comenzar la operacion de consenso sobre la operacion y devolver en
// cuanto se consiga
//
// No hay garantía que esta operación consiga comprometerse en una entrada de
// de registro, dado que el lider puede fallar y la entrada ser reemplazada
// en el futuro.
// Resultado de este método :
// - Primer valor devuelto es el indice del registro donde se va a colocar
// - la operacion si consigue comprometerse.
// - El segundo valor es el mandato en curso
// - El tercer valor es true si el nodo cree ser el lider
// - Cuarto valor es el lider, es el indice del líder si no es él
// - Quinto valor es el resultado de aplicar esta operación en máquina de estados
func (nr *NodoRaft) someterOperacion(operacion TipoOperacion) (int, int,
	bool, int, string) {
	indice := -1
	mandato := -1
	EsLider := false
	idLider := -1
	valorADevolver := ""

	// Si no soy el líder, retorno inmediatamente
	if !EsLider {
		return indice, mandato, EsLider, idLider, valorADevolver
	}

	// Soy el líder, añado la operación al log local
	nr.Mux.Lock()

	nuevaEntrada := EntradaLog{
		Mandato:   nr.Mandato,
		Operacion: operacion,
	}
	nr.log = append(nr.log, nuevaEntrada) // Añadir al log
	indice = len(nr.log) - 1              // Índice de la nueva entrada
	nr.Mux.Unlock()

	fmt.Printf("Operación loggeada en el índice %d en el líder.\n", indice)

	// Replicar la operación en los seguidores
	replicados := 1 // Contamos al líder como el primer replicado
	for i := range nr.Nodos {
		if i != nr.Yo {
			go func(i int) {
				if nr.enviarEntradasAppend(i, nr.nextIndex[i]) {
					nr.Mux.Lock()
					replicados++
					nr.Mux.Unlock()
				}
			}(i)
		}
	}

	// Esperar a que la mayoría de los nodos haya replicado la operación
	majority := (len(nr.Nodos) / 2) + 1
	for {
		nr.Mux.Lock()
		if replicados >= majority {
			nr.Mux.Unlock()
			break
		}
		nr.Mux.Unlock()
		time.Sleep(100 * time.Millisecond) // Pausa breve para esperar replicación
	}

	fmt.Printf("Operación replicada en la mayoría de nodos. Índice: %d\n", indice)

	return indice, mandato, EsLider, idLider, valorADevolver
}

// -----------------------------------------------------------------------
// LLAMADAS RPC al API
//
// Si no tenemos argumentos o respuesta estructura vacia (tamaño cero)
type Vacio struct{}

func (nr *NodoRaft) ParaNodo(args Vacio, reply *Vacio) error {
	defer nr.para()
	return nil
}

type EstadoParcial struct {
	Mandato int
	EsLider bool
	IdLider int
}

type EstadoRemoto struct {
	IdNodo int
	EstadoParcial
}

func (nr *NodoRaft) ObtenerEstadoNodo(args Vacio, reply *EstadoRemoto) error {
	reply.IdNodo, reply.Mandato, reply.EsLider, reply.IdLider = nr.obtenerEstado()
	return nil
}

type ResultadoRemoto struct {
	ValorADevolver string
	IndiceRegistro int
	EstadoParcial
}

func (nr *NodoRaft) SometerOperacionRaft(operacion TipoOperacion,
	reply *ResultadoRemoto) error {
	reply.IndiceRegistro, reply.Mandato, reply.EsLider,
		reply.IdLider, reply.ValorADevolver = nr.someterOperacion(operacion)
	return nil
}

// -----------------------------------------------------------------------
// LLAMADAS RPC protocolo RAFT
//
// Structura de ejemplo de argumentos de RPC PedirVoto.
//
// Recordar
// -----------
// Nombres de campos deben comenzar con letra mayuscula !
type ArgsPeticionVoto struct {
	// Vuestros datos aqui
	MandatoActual       int
	IdYo                int
	UltimoIndiceLogged  int
	UltimoMandatoLogged int
}

// Structura de ejemplo de respuesta de RPC PedirVoto,
//
// Recordar
// -----------
// Nombres de campos deben comenzar con letra mayuscula !
type RespuestaPeticionVoto struct {
	// Vuestros datos aqui
	VotoParaTi   bool
	MandatoReply int
}

// Metodo para RPC PedirVoto
func (nr *NodoRaft) PedirVoto(peticion *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) error {
	// Vuestro codigo aqui
	fmt.Println("pedirVoto")
	nr.Mux.Lock() // acceso a sección crítica, bloqueamos mutex

	defer nr.Mux.Unlock() // al final, lo desbloquearemos

	if peticion.MandatoActual > nr.Mandato {
		// El mandato de la petición es mayor: actualizo mi mandato y voto
		fmt.Printf("Nodo %d: Mandato actual (%d) es menor que el recibido (%d), actualizando y votando por %d.\n",
			nr.Yo, nr.Mandato, peticion.MandatoActual, peticion.IdYo)
		nr.Mandato = peticion.MandatoActual
		reply.VotoParaTi = true
	} else if peticion.MandatoActual == nr.Mandato {
		// Si los mandatos son iguales, compruebo quién tiene el último log más actualizado
		if peticion.UltimoMandatoLogged > nr.UltimoMandatoLogged ||
			(peticion.UltimoMandatoLogged == nr.UltimoMandatoLogged && peticion.UltimoIndiceLogged >= nr.UltimoIndiceLogged) {
			// Si el log de la petición es más avanzado, voto por el nodo
			fmt.Printf("Nodo %d: Mismos mandatos, pero el log del peticionario es más avanzado. Votando por %d.\n",
				nr.Yo, peticion.IdYo)
			reply.VotoParaTi = true
		} else {
			// Si mi log es más avanzado, rechazo el voto
			fmt.Printf("Nodo %d: Mismos mandatos, pero mi log es más avanzado. No voto por %d.\n",
				nr.Yo, peticion.IdYo)
			reply.VotoParaTi = false
		}
	} else {
		// El mandato de la petición es menor: no voto
		fmt.Printf("Nodo %d: Mandato actual (%d) es mayor que el recibido (%d). No voto por %d.\n",
			nr.Yo, nr.Mandato, peticion.MandatoActual, peticion.IdYo)
		reply.VotoParaTi = false
	}

	// Devolver siempre el mandato actual del nodo
	reply.MandatoReply = nr.Mandato
	return nil
}

func (nr *NodoRaft) iniciarEleccion() {

	nr.Mux.Lock()

	// incemento mandato y me voto a mi mismo, porque no se nada del resto aún
	nr.Mandato++
	nr.IdLider = -1     // no hay lider, lider = -1
	votosRecibidos := 1 // el mio propio
	nr.Logger.Printf("Iniciando elección en el mandato %d", nr.Mandato)

	args := ArgsPeticionVoto{
		MandatoActual:       nr.Mandato,
		IdYo:                nr.Yo,
		UltimoIndiceLogged:  nr.UltimoIndiceLogged,
		UltimoMandatoLogged: nr.UltimoMandatoLogged,
	}

	nr.Mux.Unlock()

	votosChannel := make(chan bool, len(nr.Nodos)-1)
	for i := range nr.Nodos {
		if i != nr.Yo {
			go func(i int) {
				var reply RespuestaPeticionVoto
				if nr.enviarPeticionVoto(i, &args, &reply) {
					votosChannel <- true
				} else {
					votosChannel <- false
				}
			}(i)
		}
	}
	timeout := time.After(500 + time.Duration(rand.Intn(500))*time.Millisecond)

	// ahora contaremos los votos
	for i := 0; i < len(nr.Nodos)-1; i++ {
		select {
		case voto := <-votosChannel:
			if voto {
				votosRecibidos++
				fmt.Printf("Nodo %d ha recibido un voto (Total: %d)\n", nr.Yo, votosRecibidos)
			}
		case <-timeout:
			nr.Logger.Println("El timeout de elección ha expirado")
			nr.Logger.Println("El timeout de elección ha expirado")
			return
		}
	}

	if votosRecibidos > len(nr.Nodos)/2 {
		nr.Mux.Lock()
		fmt.Printf("He ganado, soy el nuevo líder en el mandato %d", nr.Mandato)
		nr.IdLider = nr.Yo
		nr.Logger.Printf("He ganado, soy el nuevo líder en el mandato %d", nr.Mandato)
		nr.Mux.Unlock()
		nr.enviarHeartBeat() // comenzar a enviar latidos, porque soy el lider
	} else {
		nr.Mux.Lock()
		fmt.Println("no soy lider sadly")
		nr.Logger.Println("No he conseguido mayoría, esperando que haya otra elección")
		nr.Mux.Unlock()
	}
}

func (nr *NodoRaft) iniciarTimeoutControl() {
	go func() {
		for {
			timeout := time.Duration(300+rand.Intn(300)) * time.Millisecond
			timer := time.NewTimer((timeout))

			select {
			case <-timer.C:
				nr.Mux.Lock()

				if nr.IdLider == -1 {
					nr.Mux.Unlock()

					nr.Logger.Println("El timeout ha expirado, comenzaré una elección")
					nr.iniciarEleccion()
				}
			case <-nr.recibirLatido:
				timer.Stop()
			}
		}
	}()
}

func (nr *NodoRaft) reiniciarTimeout() {
	// Aquí se puede enviar una señal al canal recibirLatido para reiniciar el timer
	nr.recibirLatido <- struct{}{}
}

// min devuelve el valor mínimo entre dos enteros, usado por no estar min nativo en 1.18
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type ArgAppendEntries struct {
	Mandato      int             // El término actual del líder
	IdLider      int             // ID del líder que envía el latido/entrada
	PrevLogIndex int             // Índice del log inmediatamente anterior a la nueva entrada
	PrevLogTerm  int             // Término en PrevLogIndex
	Entries      []TipoOperacion // Nuevas entradas para replicar (vacío para latidos)
	LeaderCommit int             // Índice de commit del líder
}

type Results struct {
	Success       bool // True si el seguidor aceptó la entrada
	TerminoActual int  // Término actual del seguidor (puede ser mayor al del líder)
	ConfirmaLider bool // True si el seguidor reconoce al líder como válido
}

// Metodo de tratamiento de llamadas RPC AppendEntries
func (nr *NodoRaft) AppendEntries(args *ArgAppendEntries,
	results *Results) error {

	nr.Mux.Lock()

	defer nr.Mux.Unlock()

	// para comenzar, si el mandato que recibo es menor que mi mandato, rechazo latido (el no es lider)
	if args.Mandato < nr.Mandato {
		results.Success = false
		results.TerminoActual = nr.Mandato
		return nil
	}

	// luego, si el mandato es mayor al mío, no estoy con el lider real, actualizo mandato y lider
	if args.Mandato > nr.Mandato {
		nr.Mandato = args.Mandato
		nr.IdLider = args.IdLider
	}

	// confirmo que el lider es el que me ha mandado, y terminoactual es mandato (actualizado ya)
	results.ConfirmaLider = true
	results.TerminoActual = nr.Mandato

	// Reiniciar el timeout enviando una señal al canal recibirLatido
	select {
	case nr.recibirLatido <- struct{}{}:
		// Latido recibido, canal notificado
	default:
		// Si el canal está lleno, seguimos adelante (evitar bloqueo)
	}
	// si es un latido de corazón SIN ENTRADAS NUEVAS
	if len(args.Entries) == 0 {
		results.Success = true
		return nil
	}

	// Si el índice anterior al log es inválido, se rechaza
	if args.PrevLogIndex >= 0 && (args.PrevLogIndex >= len(nr.log) || nr.log[args.PrevLogIndex].Mandato != args.PrevLogTerm) {
		results.Success = false
		return nil
	}

	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		if index < len(nr.log) {
			nr.log[index] = EntradaLog{Mandato: args.Mandato, Operacion: entry} // reemplaza entrada existente, porque el indice es menor que la longitud
		} else {
			nr.log = append(nr.log, EntradaLog{Mandato: args.Mandato, Operacion: entry})
		}
	}

	// Actualizar el índice de commit si el líder tiene entradas comprometidas
	if args.LeaderCommit > nr.indiceComprometido {
		nr.indiceComprometido = min(args.LeaderCommit, len(nr.log)-1)
	}

	results.Success = true
	return nil

}

// --------------------------------------------------------------------------
// ----- METODOS/FUNCIONES desde nodo Raft, como cliente, a otro nodo Raft
// --------------------------------------------------------------------------

// Ejemplo de código enviarPeticionVoto
//
// nodo int -- indice del servidor destino en nr.nodos[]
//
// args *RequestVoteArgs -- argumentos par la llamada RPC
//
// reply *RequestVoteReply -- respuesta RPC
//
// Los tipos de argumentos y respuesta pasados a CallTimeout deben ser
// los mismos que los argumentos declarados en el metodo de tratamiento
// de la llamada (incluido si son punteros)
//
// Si en la llamada RPC, la respuesta llega en un intervalo de tiempo,
// la funcion devuelve true, sino devuelve false
//
// la llamada RPC deberia tener un timeout adecuado.
//
// Un resultado falso podria ser causado por una replica caida,
// un servidor vivo que no es alcanzable (por problemas de red ?),
// una petición perdida, o una respuesta perdida
//
// Para problemas con funcionamiento de RPC, comprobar que la primera letra
// del nombre de todo los campos de la estructura (y sus subestructuras)
// pasadas como parametros en las llamadas RPC es una mayuscula,
// Y que la estructura de recuperacion de resultado sea un puntero a estructura
// y no la estructura misma.
func (nr *NodoRaft) enviarPeticionVoto(nodo int, args *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) bool {

	fmt.Printf("Nodo %d solicitando votos: [MandatoActual: %d, IdYo: %d, UltimoIndiceLogged: %d, UltimoMandatoLogged: %d] - Respuesta esperada: [VotoParaTi: %t, MandatoReply: %d]\n",
		nodo, args.MandatoActual, args.IdYo, args.UltimoIndiceLogged, args.UltimoMandatoLogged, reply.VotoParaTi, reply.MandatoReply)

	//creo que aquí hay que añadir lo de randomtime, para que no vayan todos a la vez solicitando la primera vez y nadie resuelva
	err := rpctimeout.HostPort.CallTimeout(nr.Nodos[nodo], "NodoRaft.PedirVoto", args, reply, 300*time.Millisecond)
	//Rpctimeout (hecho en el codigo esqueleto) básicamente ejecuta lo pedido como string con "args", guarda en reply, a el nodo "nodo"
	if err == nil && reply.VotoParaTi {
		nr.Logger.Println("voto concedido por el nodo:", nodo)
		return true
	} else if err == nil && !reply.VotoParaTi {
		nr.Logger.Println("Voto no concedido por el nodo:", nodo)
		return false
	} else {
		nr.Logger.Println("Error en la llamada RPC o timeout:", nodo)
		return false
	}
}

// auxiliar max para máximo
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// aux func para extraer las operaciones de los logs
func extraerOperaciones(entradas []EntradaLog) []TipoOperacion {
	operaciones := make([]TipoOperacion, len(entradas))
	for i, entrada := range entradas {
		operaciones[i] = entrada.Operacion
	}
	return operaciones
}

func (nr *NodoRaft) enviarEntradasAppend(nodo int, indiceLog int) bool {
	nr.Mux.Lock()         // block mutex
	defer nr.Mux.Unlock() // desbloquear siempre al final

	fmt.Printf("\n----- Enviando AppendEntries -----\n")
	fmt.Printf("Nodo destino: %d\nÍndice de log a replicar: %d\n", nodo, indiceLog)

	// Calcula el índice anterior al log
	PrevLogIndex := nr.nextIndex[nodo] - 1
	fmt.Printf("PrevLogIndex calculado: %d\n", PrevLogIndex)

	// debido a que puede no haber habido mandatos, caso especial gestionando eso
	var PrevLogTerm int
	if PrevLogIndex >= 0 && PrevLogIndex < len(nr.log) {
		// Si hay entradas previas, asignar el término de la entrada previa
		PrevLogTerm = nr.log[PrevLogIndex].Mandato
		fmt.Printf("PrevLogTerm obtenido del log: %d\n", PrevLogTerm)
	} else {
		// Si no hay entradas previas, asignar 0 o algún valor especial
		PrevLogTerm = 0
		fmt.Println("PrevLogTerm asignado a 0 debido a que no hay entradas previas.")
	}

	// Validamos que el índice de log no sea mayor que la longitud de nr.log
	var entries []TipoOperacion
	if indiceLog < len(nr.log) {
		entries = extraerOperaciones(nr.log[indiceLog:])
		fmt.Printf("Entradas a replicar desde el índice %d: %+v\n", indiceLog, entries)
	} else {
		entries = []TipoOperacion{} // No hay entradas que enviar
		fmt.Printf("No hay entradas a replicar, indiceLog: %d es mayor o igual que len(nr.log): %d\n", indiceLog, len(nr.log))
	}

	args := ArgAppendEntries{ // argumentos necesarios a mandar
		Mandato:      nr.Mandato,
		IdLider:      nr.Yo,
		PrevLogIndex: PrevLogIndex,          // El índice anterior
		PrevLogTerm:  PrevLogTerm,           // Término de la entrada anterior
		Entries:      entries,               // Enviar desde el índice especificado en adelante
		LeaderCommit: nr.indiceComprometido, // Comprometido hasta este punto
	}

	fmt.Printf("----- Argumentos enviados en AppendEntries -----\nMandato: %d\nIdLider: %d\nPrevLogIndex: %d\nPrevLogTerm: %d\nEntries: %+v\nLeaderCommit: %d\n",
		args.Mandato, args.IdLider, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit)

	// Realiza la llamada RPC para enviar las entradas
	var reply Results
	err := rpctimeout.HostPort.CallTimeout(nr.Nodos[nodo], "NodoRaft.AppendEntries", args, &reply, 500*time.Millisecond)
	fmt.Println("HE HECHO EL CALLTIMEOUT RPCTIMPEOUTS")
	if err != nil || !reply.Success {
		nr.nextIndex[nodo] = max(1, nr.nextIndex[nodo]-1) // reducir el nextIndex si falla
		return false
	}

	// Si se ha enviado correctamente
	nr.nextIndex[nodo] = len(nr.log)      // Actualizamos el siguiente índice
	nr.matchIndex[nodo] = len(nr.log) - 1 // Actualizamos matchIndex
	nr.Logger.Printf("Nodo %d ha replicado correctamente hasta el índice %d", nodo, indiceLog)

	return true
}

// función que envia heartbeats a los nodos que no son yo, si yo soy el lider
func (nr *NodoRaft) enviarHeartBeat() {
	go func() {
		ticker := time.NewTicker(1500 * time.Millisecond)
		defer ticker.Stop()
		fmt.Println("\n----- Iniciando Heartbeats -----")
		fmt.Println("\n----- Enviando Heartbeats -----")
		fmt.Println("Mutex adquirido. Comprobando si soy líder...")

		for {
			select {
			case <-ticker.C:
				nr.Mux.Lock()

				if nr.IdLider != nr.Yo { // Si ya no es líder, terminamos
					nr.Mux.Unlock()
					fmt.Println("Ya no soy el líder. Mutex liberado y salgo de la rutina de heartbeats.")
					return
				}
				fmt.Println("Soy el líder. Procediendo a enviar heartbeats a los seguidores.")

				// Enviar latidos a todos los seguidores
				for i := 0; i < len(nr.Nodos); i++ {
					if i != nr.Yo {
						fmt.Printf("Envío heartbeats al nodo %d\n", i)
						go nr.enviarEntradasAppend(i, nr.nextIndex[i])
					} else {
						fmt.Printf("No se envía heartbeat a mí mismo (nodo %d)\n", i)
					}
				}
				nr.Mux.Unlock() // desbloqueamos el mutex
				fmt.Println("Heartbeats enviados, mutex liberado.")

			}
		}
	}()
}
