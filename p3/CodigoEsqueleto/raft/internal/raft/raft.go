package raft

import (
	"fmt"
	"io"
	"log"
	"os"

	//"crypto/rand"
	"sync"
	"time"

	//"net/rpc"
	"raft/internal/comun/rpctimeout"
	"raft/internal/comun/utils"
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

	// Conjunto de estados de nodo
	Seguidor  = "seguidor"
	Parado    = "parado"
	Candidato = "candidato"
	Lider     = "lider"

	lectura   = "leer"
	escritura = "escribir"
)

// aunque no se este leyendo ni escribiendo, para la realización del algoritmo, es más sencillo
// plantearlo como el algoritmo completo, que como secciones que tienen que funcionar del mismo
type TipoOperacion struct {
	Operacion string // La operaciones posibles son "leer" y "escribir"
	Clave     string
	Valor     string // en el caso de la lectura Valor = ""
}

// A medida que el nodo Raft conoce las operaciones de las  entradas de registro
// comprometidas, debería enviar un AplicaOperacion a la máquina de estados
type AplicaOperacion struct {
	Indice    int // en la entrada de registro
	Mandato   int
	Operacion TipoOperacion
}

// tipo para guardar operaciones
type EntradaLog struct {
	Mandato   int
	Operacion TipoOperacion
}

type Estado struct {
	MandatoActual      int
	HeVotadoA          int
	Log                []AplicaOperacion
	indiceComprometido int
	UltimoIndiceLogged int
	nextIndex          []int
	matchIndex         []int
}

// Tipo de dato Go que representa un solo nodo (réplica) de raft
type NodoRaft struct {
	Mux             sync.Mutex            // Mutex para proteger acceso a estado compartido
	Nodos           []rpctimeout.HostPort // Host:Port de todos los nodos (réplicas) Raft, en mismo orden
	Yo              int                   // indice de este nodos en campo array "nodos"
	IdLider         int                   // lider que considero actual
	Logger          *log.Logger
	Estado          Estado
	RolActual       string
	CanalVotos      chan bool
	CanalHeartBeats chan bool
	Exito           chan bool
	AplicaOperacion chan int

	AlmacenDeOperaciones map[string]string
}

func CrearLogger(nodos []rpctimeout.HostPort, yo int) *log.Logger {
	var logger *log.Logger
	if kEnableDebugLogs {
		nombreNodo := nodos[yo].Host() + "_" + nodos[yo].Port()

		if kLogToStdout {
			logger = log.New(os.Stdout, nombreNodo+" -->> ",
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
			logger = log.New(logOutputFile,
				nombreNodo+" -> ", log.Lmicroseconds|log.Lshortfile)
		}
		logger.Println("logger initialized")
	} else {
		logger = log.New(io.Discard, "", 0)
	}
	return logger
}

func (nr *NodoRaft) iniciarEstado() {
	nr.RolActual = Seguidor
	nr.IdLider = IntNOINICIALIZADO // aunque ya haya sido puesto en nuevo nodo, aseguramos que no tenga lider

	// Estado actual
	nr.Estado.MandatoActual = 0
	nr.Estado.HeVotadoA = IntNOINICIALIZADO
	nr.Estado.Log = nil
	nr.Estado.indiceComprometido = IntNOINICIALIZADO
	nr.Estado.UltimoIndiceLogged = IntNOINICIALIZADO
	nr.Estado.nextIndex = utils.Make(0, len(nr.Nodos))
	nr.Estado.matchIndex = utils.Make(IntNOINICIALIZADO, len(nr.Nodos))

	// Inicializar canales
	nr.CanalVotos = make(chan bool)
	nr.CanalHeartBeats = make(chan bool)
	nr.Exito = make(chan bool)
	nr.AplicaOperacion = make(chan int)
	nr.AlmacenDeOperaciones = make(map[string]string)

	go func() {
		for indice := range nr.AplicaOperacion {
			nr.aplicarOperacion(indice)
		}
	}()
}

// Creacion de un nuevo nodo de eleccion
// Tabla de <Direccion IP:puerto> de cada nodo incluido a si mismo. <Direccion IP:puerto> de este nodo esta en nodos[yo]
// canalAplicar es un canal donde, en la practica 5, se recogerán las operaciones a aplicar a la máquina de estados. Se puede asumir que este canal se consumira de forma continúa.
// NuevoNodo() debe devolver resultado rápido, por lo que se deberían poner en marcha Gorutinas para trabajos de larga duracion
func NuevoNodo(nodos []rpctimeout.HostPort, yo int, canalAplicarOperacion chan AplicaOperacion) *NodoRaft {
	nr := &NodoRaft{}
	nr.Nodos = nodos
	nr.Yo = yo
	nr.IdLider = IntNOINICIALIZADO
	nr.Logger = CrearLogger(nodos, yo)

	nr.iniciarEstado()

	go nr.appLoop()

	return nr
}

// Metodo Para() utilizado cuando no se necesita mas al nodo
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
	yo := nr.Yo
	mandato := nr.Estado.MandatoActual
	esLider := nr.RolActual == Lider
	idLider := IntNOINICIALIZADO
	if esLider {
		idLider = nr.Yo
	} else {
		idLider = nr.IdLider
	}
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

	// todo sin inicializar inicialmente
	indice := IntNOINICIALIZADO
	mandato := IntNOINICIALIZADO
	EsLider := false
	idLider := IntNOINICIALIZADO
	valorADevolver := ""

	// Si no soy el líder, retorno inmediatamente
	if nr.RolActual != Lider {
		return indice, mandato, EsLider, idLider, valorADevolver
	}

	// appendo la información a mi log porque soy el lider
	indice = len(nr.Estado.Log)
	mandato = nr.Estado.MandatoActual
	EsLider = true
	idLider = nr.Yo
	nr.Estado.Log = append(nr.Estado.Log, AplicaOperacion{Indice: indice, Mandato: mandato, Operacion: operacion})
	nr.Logger.Printf("Operación ha sido añadida al log: %v", operacion)
	return indice, mandato, EsLider, idLider, valorADevolver
}

// Actualizar log, necesario para cuando el seguidor no tenga el mismo log que el lider, hay que actualizarlo
func (nr *NodoRaft) actualizarLog(logLength int, leaderCommit int,
	entries []AplicaOperacion) {
	// explicación del código de forma humana:

	// eliminar comflictos -> Si el seguidor tiene entradas que no coinciden con las del líder, se eliminarán
	// agregar entradas que faltan -> Si el log del seguidor es más corto, se añaden las entradas del líder.
	// aplicar entradas nuevas que se han mandado como comprometidas -> Si el líder ha marcado nuevas entradas como comprometidas,
	// se aplican a la máquina de estados del seguidor.
	// Tras esta funcion, está sincronizado el log y el estado comprometido del líder.

	if len(entries) == 0 && leaderCommit <= nr.Estado.indiceComprometido {
		nr.Logger.Println("Latido recibido sin cambios en el log")
		return
	}

	if len(entries) > 0 && len(nr.Estado.Log) > logLength {
		if logLength == entries[0].Indice && nr.Estado.Log[logLength].Mandato == entries[0].Mandato {
			nr.Logger.Printf("Conflicto t1: coincide índice y mandato, corto log a %d", logLength)
			nr.Estado.Log = nr.Estado.Log[:logLength]
		} else if nr.Estado.Log[logLength].Mandato != entries[0].Mandato {
			nr.Logger.Printf("Conflicto t2: mandato no coincide en %d, corto log", logLength)
			nr.Estado.Log = nr.Estado.Log[:logLength]
		}
	}

	if (logLength + len(entries)) > len(nr.Estado.Log) {
		nr.Logger.Println("Añadimos las entradas entries al log")
		nr.Estado.Log = append(nr.Estado.Log, entries...)
	}

	if leaderCommit > nr.Estado.indiceComprometido {
		nr.Logger.Println("Aplicaremos a la máquina de estados aquellas entradas no aplicadas (no eran comunes)")
		siguiente := nr.Estado.indiceComprometido + 1
		nr.Estado.indiceComprometido = leaderCommit

		for i := siguiente; i <= nr.Estado.indiceComprometido; i++ {
			nr.AplicaOperacion <- i
		}

	}
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
	IdNodo       int
	Mandato      int
	EsLider      bool
	IdLider      int
	IndiceCommit int
}

func (nr *NodoRaft) ObtenerEstadoNodo(args Vacio, reply *EstadoRemoto) error {
	reply.IdNodo, reply.Mandato, reply.EsLider, reply.IdLider = nr.obtenerEstado()
	return nil
}

// estructura para el estado del almacenamiento, se usa en el RPC ObtenerEstadoAlmacen
type EstadoAlmacen struct {
	IdNodo  int
	Log     []AplicaOperacion
	Almacen map[string]string
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
	CandidatoId         int
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

	nr.Mux.Lock() // acceso a sección crítica, bloqueamos mutex
	miTerminoLoggeado := 0
	LogLenght := len(nr.Estado.Log)

	if LogLenght > 0 {
		miTerminoLoggeado = nr.Estado.Log[LogLenght-1].Mandato
	}

	// comprobación de si el log está bien, tanto ultimo logeado como si el ultimo indice es mayor al menos que longitud del log
	logIsOK := peticion.UltimoMandatoLogged > miTerminoLoggeado || (peticion.UltimoMandatoLogged == miTerminoLoggeado && peticion.UltimoIndiceLogged >= LogLenght-1)
	termIsOK := peticion.MandatoActual > nr.Estado.MandatoActual || (peticion.MandatoActual == nr.Estado.MandatoActual &&
		(nr.Estado.HeVotadoA == IntNOINICIALIZADO || nr.Estado.HeVotadoA == peticion.CandidatoId))

	reply.MandatoReply = nr.Estado.MandatoActual
	if logIsOK && termIsOK {
		nr.Logger.Printf("Recive RPC.PedirVoto: Voto concedido a %d\n", peticion.CandidatoId)
		nr.Estado.MandatoActual = peticion.MandatoActual
		nr.Estado.HeVotadoA = peticion.CandidatoId
		nr.RolActual = Seguidor
		nr.CanalHeartBeats <- true
		reply.VotoParaTi = true
		reply.MandatoReply = nr.Estado.MandatoActual
	} else {
		reply.VotoParaTi = false
	}
	nr.Mux.Unlock()

	return nil
}

type ArgAppendEntries struct {
	Mandato      int             // El mandato actual
	IdLider      int             // ID del líder que envía el latido
	PrevLogIndex int             // Índice del log  previo
	PrevLogTerm  int             // Mandato en PrevLogIndex
	Entries      []TipoOperacion // Nuevas entradas para replicar (vacío para latidos)
	LeaderCommit int             // Índice de commit del líder
}

type Results struct {
	Success       bool // True si el seguidor aceptó la entrada
	TerminoActual int  // Término actual del seguidor (puede ser mayor al del líder)
	MatchIndex    int
}

// convierte Entries a vector de AplicaOperación, se podría implementar de tal forma que no fuera necesario
func convertToAplicaOperacion(entries []TipoOperacion, startIndex int, mandato int) []AplicaOperacion {
	aplicaEntries := make([]AplicaOperacion, len(entries))
	for i, entry := range entries {
		aplicaEntries[i] = AplicaOperacion{
			Indice:    startIndex + i,
			Mandato:   mandato,
			Operacion: entry,
		}
	}
	return aplicaEntries
}

// Metodo de tratamiento de llamadas RPC AppendEntries
func (nr *NodoRaft) AppendEntries(args *ArgAppendEntries,
	results *Results) error {

	nr.Logger.Printf("AppendEntries fue recibido de %d", args.IdLider)

	if args.Mandato > nr.Estado.MandatoActual {
		nr.Logger.Println("RECV RPC.AppendEntries: Voy por detras")
		nr.Estado.MandatoActual = args.Mandato
		nr.Estado.HeVotadoA = IntNOINICIALIZADO

		aplicaEntries := convertToAplicaOperacion(args.Entries, args.PrevLogIndex+1, args.Mandato)
		nr.actualizarLog(args.PrevLogIndex+1, args.LeaderCommit, aplicaEntries)

		if args.LeaderCommit > nr.Estado.indiceComprometido {
			nr.Logger.Println("Actualizando índice de compromiso basado en LeaderCommit")
			nr.Estado.indiceComprometido = min(args.LeaderCommit, len(nr.Estado.Log)-1)

			for i := nr.Estado.indiceComprometido + 1; i <= nr.Estado.indiceComprometido; i++ {
				nr.AplicaOperacion <- i
			}
			nr.Logger.Println("Entradas comprometidas y aplicadas")
		}

		results.Success = true
		results.TerminoActual = nr.Estado.MandatoActual
		results.MatchIndex = args.PrevLogIndex + len(args.Entries)
		nr.CanalHeartBeats <- true
	} else {
		nr.Logger.Printf("AppendEntries fallido: %v", args)
		results.TerminoActual = nr.Estado.MandatoActual
		results.Success = false
		results.MatchIndex = -1
	}

	return nil

}

// funcion para calcular el mínimo de dos ints
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
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

	//creo que aquí hay que añadir lo de randomtime, para que no vayan todos a la vez solicitando la primera vez y nadie resuelva
	err := nr.Nodos[nodo].CallTimeout("NodoRaft.PedirVoto", args, reply, 25*time.Millisecond)

	nr.Logger.Printf("Se ha realizado RPC.PedirVoto: Request[%d]{%v} -> Reply[%d]{%v}", nr.Yo, args, nodo, reply)
	return err == nil
}

func (nr *NodoRaft) enviarEntradasAppend(nodo int, args *ArgAppendEntries, reply *Results) bool {

	err := nr.Nodos[nodo].CallTimeout("NodoRaft.AppendEntries", args, reply, 25*time.Millisecond)

	nr.Logger.Printf("RPC.AppendEntries: Request[%d]{%v} -> Reply[%d]{%v}", nr.Yo, args, nodo, reply)
	return err == nil
}

func (nr *NodoRaft) iniciarEleccion() {

	nr.Estado.HeVotadoA = nr.Yo
	nr.Estado.MandatoActual++

	args := ArgsPeticionVoto{nr.Estado.MandatoActual, nr.Yo, -1, 0}
	if len(nr.Estado.Log) > 0 {
		args.UltimoIndiceLogged = len(nr.Estado.Log) - 1
		args.UltimoMandatoLogged = nr.Estado.Log[args.UltimoIndiceLogged].Mandato

	}

	for nodos := range nr.Nodos {
		if nodos != nr.Yo {
			var respuesta RespuestaPeticionVoto
			go nr.peticionVoto(nodos, &args, &respuesta)
		}
	}
}

func (nr *NodoRaft) peticionVoto(nodo int,
	args *ArgsPeticionVoto, reply *RespuestaPeticionVoto) {
	if nr.enviarPeticionVoto(nodo, args, reply) {
		nr.Mux.Lock()
		if nr.RolActual == Candidato && reply.MandatoReply == nr.Estado.MandatoActual && reply.VotoParaTi {
			nr.Logger.Printf("recivo RPC.PedirVoto: Voto recibido de %d\n", nodo)
			nr.CanalVotos <- true
		} else if reply.MandatoReply > nr.Estado.MandatoActual {
			nr.Logger.Println("recivo RPC.PedirVoto: voy por detras")
			nr.RolActual = Seguidor
			nr.Estado.MandatoActual = reply.MandatoReply
			nr.Estado.HeVotadoA = -1
		}
		nr.Mux.Unlock()
	} else {
		nr.Logger.Println("recivo RPC.PedirVoto: ERROR al recibir el mensaje")
	}
}

func (nr *NodoRaft) solicitoLatido(nodo int, args *ArgAppendEntries, respuesta *Results) {
	for {
		if !nr.enviarEntradasAppend(nodo, args, respuesta) {
			nr.Logger.Println("Recive RPC.AppendEntries: ERROR al recibir mensaje")
			return
		}

		if respuesta.TerminoActual > nr.Estado.MandatoActual {
			nr.Logger.Println("Recive RPC.AppendEntries: Voy rezagado")
			nr.RolActual = Seguidor
			nr.Estado.MandatoActual = respuesta.TerminoActual
			nr.Estado.HeVotadoA = -1
			return
		}

		if respuesta.Success {
			if len(args.Entries) > 0 {
				nr.Logger.Printf("Recive RPC.AppendEntries: OK -> seguimos "+
					"NextIndex:%d -> NextIndex:%d y MatchIndex:%d -> MatchIndex:%d\n",
					nr.Estado.nextIndex[nodo], respuesta.MatchIndex+1,
					nr.Estado.matchIndex[nodo], respuesta.MatchIndex)
			} else {
				nr.Logger.Println("Recive RPC.AppendEntries: OK")
			}
			nr.Estado.nextIndex[nodo] = respuesta.MatchIndex + 1
			nr.Estado.matchIndex[nodo] = respuesta.MatchIndex
			nr.ComprometerEntradasLog()
			break
		} else {
			// Retroceder más en el log si es necesario
			nr.Logger.Printf("Recive RPC.AppendEntries: NOT OK -> retrocedemos "+
				"NextIndex:%d -> NextIndex:%d", nr.Estado.nextIndex[nodo], nr.Estado.nextIndex[nodo]-1)
			nr.Estado.nextIndex[nodo]--
			if nr.Estado.nextIndex[nodo] < 0 {
				break
			}
			// Actualizar `args` para el siguiente intento
			args.PrevLogIndex = nr.Estado.nextIndex[nodo] - 1
			if args.PrevLogIndex >= 0 && args.PrevLogIndex < len(nr.Estado.Log) {
				args.PrevLogTerm = nr.Estado.Log[args.PrevLogIndex].Mandato
			}
			args.Entries = convertToTipoOperacion(nr.Estado.Log[nr.Estado.nextIndex[nodo]:])
		}
	}
}

// función que envia heartbeats a los nodos que no son yo, si yo soy el lider
func (nr *NodoRaft) EnviarHeartBeat() {
	for nodo := range nr.Nodos {
		if nodo != nr.Yo {
			args := ArgAppendEntries{
				Mandato:      nr.Estado.MandatoActual,
				IdLider:      nr.Yo,
				PrevLogIndex: nr.Estado.nextIndex[nodo] - 1,
				PrevLogTerm:  0,
				Entries:      nil,
				LeaderCommit: nr.Estado.indiceComprometido,
			}

			for nodo := range nr.Nodos {
				if nodo != nr.Yo {
					var reply Results
					args.PrevLogIndex = nr.Estado.nextIndex[nodo] - 1
					if args.PrevLogIndex > -1 && args.PrevLogIndex < len(nr.Estado.Log) {
						args.PrevLogTerm = nr.Estado.Log[args.PrevLogIndex].Mandato
					}

					if nr.Estado.nextIndex[nodo] < len(nr.Estado.Log) {
						args.Entries = convertToTipoOperacion(nr.Estado.Log[nr.Estado.nextIndex[nodo]:])
					} else {
						args.Entries = nil
					}
					go nr.solicitoLatido(nodo, &args, &reply)
				}
			}
		}
	}
}

func convertToTipoOperacion(entries []AplicaOperacion) []TipoOperacion {
	ops := make([]TipoOperacion, len(entries))
	for i, entry := range entries {
		ops[i] = entry.Operacion
	}
	return ops
}

func (nr *NodoRaft) contarAcks(index int) int {
	acks := 1
	for nodo := range nr.Nodos {
		if nodo != nr.Yo && nr.Estado.matchIndex[nodo] >= index {
			acks++
		}
	}
	return acks
}

func (nr *NodoRaft) ComprometerEntradasLog() {
	minAcks := len(nr.Nodos) / 2
	for i := nr.Estado.indiceComprometido + 1; i < len(nr.Estado.Log); i++ {
		acks := nr.contarAcks(i)
		if acks > minAcks && nr.Estado.Log[i].Mandato <= nr.Estado.MandatoActual {
			nr.Estado.indiceComprometido = i
			nr.AplicaOperacion <- i
		}
	}
}

// gestión de la máquina de estados

func (nr *NodoRaft) appLoop() {

	nr.Logger.Printf("YO SOY EL NIGGA, HE EMPEZADO: %d\n", nr.Yo)

	for {
		switch nr.RolActual { // switch en el que gestionamos que rol soy en cada momento, y que hago con ello
		case Seguidor:
			nr.Logger.Printf("Estado: Seguidor, Mandato: %d\n", nr.Estado.MandatoActual)
			nr.seguidorLoop()
		case Candidato:
			nr.Logger.Printf("Estado: Candidato a lider, Mandato: %d\n", nr.Estado.MandatoActual)
			nr.candidatoLoop()
		case Lider:
			nr.Logger.Printf("Estado: Lider, Mandato: %d\n", nr.Estado.MandatoActual)
			nr.liderLoop()
		}
	}
}

func (nr *NodoRaft) seguidorLoop() {
	timer := time.NewTimer(utils.ElectionTimeout())
	defer timer.Stop()

	for nr.RolActual == Seguidor {
		select {
		case <-nr.CanalHeartBeats:
			timer.Reset(utils.ElectionTimeout())
		case <-timer.C:
			nr.RolActual = Candidato
		}
	}
}

/**
 * @brief Caso de que el nodo sea un candidato.
 */
func (nr *NodoRaft) candidatoLoop() {
	ticker := time.NewTicker(utils.ElectionTimeout())
	defer ticker.Stop()

	votosRecibidos := 1
	nr.iniciarEleccion()

	for nr.RolActual == Candidato {
		select {
		case <-nr.CanalVotos:
			votosRecibidos++
			if votosRecibidos > len(nr.Nodos)/2 {
				nr.convertirALider()
			}
		case <-ticker.C:
			votosRecibidos = 1
			nr.iniciarEleccion()
		}
	}
}

/**
 * @brief Caso de que el nodo sea un líder.
 */
func (nr *NodoRaft) liderLoop() {
	nr.Logger.Printf("Inicializando líder: Indice Comprometido:%d Log: %v\n",
		nr.Estado.indiceComprometido, nr.Estado.Log)

	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()

	nr.convertirALider()

	nr.EnviarHeartBeat()

	for nr.RolActual == Lider {
		select {
		case <-ticker.C:
			nr.EnviarHeartBeat()

		}
	}
}

func (nr *NodoRaft) convertirALider() {
	nr.RolActual = Lider
	nr.IdLider = nr.Yo
	nr.Estado.nextIndex = utils.Make(len(nr.Estado.Log), len(nr.Nodos))
	nr.Estado.matchIndex = utils.Make(-1, len(nr.Nodos))
	nr.EnviarHeartBeat()
}

// Definir una estructura para la respuesta RPC
type RespuestaIndiceComprometido struct {
	CommitIndex int
}

// Método RPC para obtener el índice de compromiso de un nodo
func (nr *NodoRaft) ObtenerIndiceComprometido(args Vacio, reply *RespuestaIndiceComprometido) error {
	nr.Mux.Lock()
	defer nr.Mux.Unlock()

	reply.CommitIndex = nr.Estado.indiceComprometido
	return nil
}

func (nr *NodoRaft) aplicarOperacion(indice int) {
	nr.Mux.Lock()
	defer nr.Mux.Unlock()

	if indice < len(nr.Estado.Log) {
		operacion := nr.Estado.Log[indice]
		nr.Logger.Printf("Aplicando operación a la máquina de estados: %v\n", operacion.Operacion)

		// Aquí deberías aplicar la operación a la máquina de estados
		// Por ejemplo:
		if operacion.Operacion.Operacion == "escribir" {
			nr.Logger.Printf("Escribiendo clave %s con valor %s\n", operacion.Operacion.Clave, operacion.Operacion.Valor)
		} else if operacion.Operacion.Operacion == "leer" {
			nr.Logger.Printf("Leyendo clave %s\n", operacion.Operacion.Clave)
		}
	}
}
