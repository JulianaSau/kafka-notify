package main

import "fmt"

func main() {
	// most basic type with a single condition
	i := 1
	for i <= 3 {
		fmt.Println(i)

		i = i + 1
	}

	// classic initial/condition/after for loop
	for j := 0; j < 3; j++ {
		fmt.Println(j)
	}

	// "do this N times" using range over an integer
	for i := range 3 {
		fmt.Println("range", i)
	}

	//for without a condition will loop repeatedly until you break out of the loop or return from the enclosing function.
    for {
		fmt.Println("loop")
		break
	}

	// You can also continue to the next iteration of the loop
	for n := range 6 {
		if n%2 == 0 {
			continue
		}

		fmt.Println(n)
	}

	// Test: print even numbers between 0-20
	for i := 0; i < 20; i++ {
		if i%2 == 0 {
			fmt.Println(i)
		} 
	}

	for i := range 20 {
		if i%2 == 0 {
            fmt.Println("range", i)
        }
		
	}
}