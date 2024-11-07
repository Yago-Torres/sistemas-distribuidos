package utils

import (
	"math/rand"
	"time"
)

// semilla para generar números aleatorios
var seed = rand.NewSource(time.Now().UnixNano())
var generator = rand.New(seed)

// ElectionTimeout especifica el tiempo para comenzar una nueva elección.
func ElectionTimeout() time.Duration {
	return time.Duration(generator.Intn(400)+100) * time.Millisecond // 100-500ms
}

/**
 * @brief Crea un vector de tamaño <num_nodos> con todos sus elementos a <val>.
 * @param val entero con el que inicializar el vector.
 * @param len entero que marca la longitud del vector.
 * @return Vector de tamaño n con todos sus elementos a <val>.
 */
func Make(val, len int) []int {
	v := make([]int, len)
	for i := range v {
		v[i] = val
	}
	return v
}

/**
 * @brief Devuelve True si el elemento a está en el vector v.
 *  			False en caso contrario.
 * @param a entero a buscar.
 * @param v Vector de enteros.
 * @return True si el elemento <a> esta dentro del vector <v>.
 */
func EstaEnLista(a int, v []int) bool {
	for _, b := range v {
		if b == a {
			return true
		}
	}
	return false
}
