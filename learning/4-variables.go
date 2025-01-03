package main

import "fmt"

func main (){

	// var declares a variable
	var a = "initial"
	fmt.Println(a)

	// You can declare multiple variables at once.
	var b, c int = 1, 2
	fmt.Println(b, c)

	// Go will infer the type of initialized variables.
	var d = true
	fmt.Println(d)

	// Zero value for each type is assigned to a variable when declared without an explicit initializer.
	var e int
	fmt.Println(e)

	// shorthand for declaring and initializing a variable
	//This syntax is only available inside functions.
	f := "apple"
	fmt.Println(f)
}