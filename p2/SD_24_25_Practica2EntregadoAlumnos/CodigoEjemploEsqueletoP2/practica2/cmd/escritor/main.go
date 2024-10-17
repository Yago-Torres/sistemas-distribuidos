package main

import (
	"fmt"
	"os"
	"os/exec"
	"practica2/ra"
	"strconv"
	"strings"
)

func EscribirFichero(fragmento string, nombreFichero string) {
	// Abrimos el fichero en modo de añadir (append), si no existe lo creamos.
	file, err := os.OpenFile(nombreFichero, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("no se pudo abrir el fichero: %w", err)
	}
	defer file.Close()

	// Escribimos el fragmento al final del fichero.
	_, err = file.WriteString(fragmento)
	if err != nil {
		fmt.Println("no se pudo escribir en el fichero: %w", err)
	}
}

func main() {
	args := os.Args
	me, err := strconv.Atoi(args[1])
	if err != nil || me < 1 {
		fmt.Println("Número de proceso mal definido,", me)
	}
	ra := ra.New(me, "../../ms/users.txt", "ESCRITURA")
	for i := 0; i < 5; i++ {
		ra.PreProtocol()
		fmt.Println(me, " entra en la sección crítica ESCRITURA")
		escribir := "\n" + "Escritura de " + strconv.Itoa(me)
		EscribirFichero(escribir, "../../fichero.txt")
		for i, peer := range ra.Ms.Peers {
			if i+1 != me {
				ip := strings.Split(peer, ":")[0]
				path := "distribuidos/practica2/"
				destination := ip + ":" + path
				fmt.Println("scp", "../fichero.txt", destination)
				cmd := exec.Command("scp", "../../fichero.txt", destination)
				err := cmd.Run()
				if err != nil {
					fmt.Println("Error copiando el fichero")
				}
			}
		}
		ra.PostProtocol()
	}
}
