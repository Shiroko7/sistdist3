package main

import (
	"fmt"
	"bufio"
	"os"
	"strings"
	"context"
	"time"
	"strconv"
	
	"google.golang.org/grpc"
	pb "starwars"
)

const (
	brokerAddress = "localhost:60051"	//Dirección y puerto de Broker
)

var (
	debugging = true					//Indica si se deben imprimir mensajes de debugging
	queryLog = make(map[string][]int)	//Registro de peticiones hechas. Tiene la forma "Ciudad@Planeta":{v,x,y,z}
										//donde v es el último valor devuelto por el servidor, y x,y,z son los
										//valores del reloj de vector retornado con la petición
)

//Imprime mensajes de debugging
func debug(s string){
	if debugging{fmt.Print(s)}
}

//Imprime mensajes de debugging
func debugLn(s string){
	if debugging{
		fmt.Println(s)
		fmt.Print(">")
	}
}


func main(){
	fmt.Println("Iniciando Querier");
	fmt.Println("Conectando a Broker... ");
	conn, err := grpc.Dial(brokerAddress, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		fmt.Printf("[Querier.main] No se pudo conectar a Broker: %v", err)
	} else {
		fmt.Println("Listo.")
	}
	defer conn.Close()
	c := pb.NewBrokerClient(conn)

	scanner := bufio.NewScanner(os.Stdin);
	running:=true
	for running{
		fmt.Print(">")
		scanner.Scan()
		params:= strings.Split(scanner.Text(), " ")
		params[0]=strings.ToUpper(params[0])
		var r *pb.Reply
		err = nil
		//n_value:="0"
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*300)
		switch params[0]{
			case "HELP":
				fmt.Println("Los siguientes comandos son reconocidos por Player:")
				fmt.Println("EXIT                      Cierra el programa Player")
				fmt.Println("HELP                      Muestra este mensaje")
				fmt.Println("GETNUMBERREBELDS [P] [C]  Solicita la cantidad de rebeldes en la ciudad C del planeta P")
			case "EXIT":
				fmt.Println("Cerrando Querier.")
				running = false
			case "GETNUMBERREBELDS":
				if len(params)!=3{
					fmt.Println("El comando "+params[0]+" requiere 2 parámetros (Planeta, Ciudad)")
					break
				}
				r, err = c.RequestRebels(ctx, &pb.RequestRebel{PlanetName:params[1],CityName:params[2]})
			default:
				fmt.Println("Comando desconocido. Use HELP para ver una lista de comandos.")
		}
		if r.GetReply()!=""{
			r_params:=strings.Split(r.GetReply(),";")
			r_sId:=0
			r_clock:=[]int{0,0,0,0}
			for _, p := range r_params{
				pList := strings.Split(p,":")
				comm := pList[0]
				value := ""
				if len(pList)>1{
					value = pList[1]
				}
				debugLn("Response: ["+p+"]")
				switch comm{
					case "CLCK":
						//fmt.Println("Received clock ["+value+"]")
						clkVals:=strings.Split(value,",")
						r_clock[1],_=strconv.Atoi(clkVals[0])
						r_clock[2],_=strconv.Atoi(clkVals[1])
						r_clock[3],_=strconv.Atoi(clkVals[2])
					case "SERV":
						//fmt.Println("Reply from server with ID ["+value+"]")
						r_sId,_=strconv.Atoi(value)
					case "RVAL":
						if value=="-1"{
							fmt.Println("No se encontró registro de la ciudad indicada")
						} else {
							fmt.Println("Rebeldes registrados en "+params[2]+"("+params[1]+"): "+value)
						}
						r_clock[0],_=strconv.Atoi(value)
				}
			}
			key:=params[2]+"@"+params[1]
			if oldClock, ok := queryLog[key]; ok {
				//check if value is valid
				oldValue:=oldClock[0]
				oldCVal:=oldClock[r_sId+1]
				if r_clock[0] != oldClock[0]{		//if values are different, new value might be obsolete
					if oldCVal > r_clock[r_sId+1]{	//compare stored and received vector clocks
						r_clock[0] = oldValue		//stored is higher, so received value is obsolete. do not replace
					}
				}
				queryLog[key]=r_clock
			} else {
				queryLog[key]=r_clock
			}
		}
		if err != nil {
			fmt.Printf("[Querier.main] No se pudo enviar mensaje: %v", err)
		}
		cancel()
	}
}