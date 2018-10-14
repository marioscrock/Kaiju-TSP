package main

import "net"
import "fmt"
import "bufio"
import "os"

//Example: {"events":[{"event":{"commit":"32fsdf3234","commitMsg":"Wrong commit!","type":"CommitEvent"}}]}
func main() {

  // connect to this socket
  conn, _ := net.Dial("tcp", "127.0.0.1:9876")
  for { 
    // read in input from stdin
    reader := bufio.NewReader(os.Stdin)
    fmt.Print("Text to send: ")
    text, _ := reader.ReadString('\n')
    // send to socket
    fmt.Fprintf(conn, text)
  }
}
