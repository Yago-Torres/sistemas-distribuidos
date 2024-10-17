package main

import (
	"bufio"
	"fmt"
	"os"
	"practica2/ra"
	"strconv"
)

func LeerFichero(nombreFichero string) (string, error) {
	file, err := os.Open(nombreFichero)
	if err != nil {
		fmt.Println("No se pudo leer el fichero,", nombreFichero)
		return "", err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	var text string
	for scanner.Scan() {
		linea := scanner.Text()
		if linea != "" {
			text += linea
			text += "\n"
		}
	}
	return text, nil
}
func main() {
	args := os.Args
	me, err := strconv.Atoi(args[1])
	if err != nil || me < 1 {
		fmt.Println("Número de proceso mal definido,", me)
	}
	ra := ra.New(me, "../../ms/users.txt", "LECTURA")
	for i := 0; i < 5; i++ {
		fmt.Println(me, "va al preprotocol")
		ra.PreProtocol()
		fmt.Println(me, " entra en la sección crítica LECTURA")
		contenido, err := LeerFichero("../../fichero.txt")
		if err != nil {
			fmt.Println(me, "no ha podido leer el fichero")
		}
		fmt.Println(contenido)
		ra.PostProtocol()
	}
}
