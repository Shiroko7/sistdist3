package main

import (
	"fmt"
	"bufio"
	"os"
	"strings"
	"context"
	"time"
	//"strconv"
	
	"google.golang.org/grpc"
	pb "starwars"
)

const (
	brokerAddress = "localhost:60051"	//Dirección y puerto de Broker
)

var (
	debugging = true	//Indica si se deben imprimir mensajes de debugging
	state = "WAKE"		//Estado actual. Se usa para control de flujo
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
	fmt.Println("Iniciando Informant");
	fmt.Println("Conectando a Broker... ");
	conn, err := grpc.Dial(brokerAddress, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		fmt.Printf("[Informant.main] No se pudo conectar a Broker: %v", err)
	} else {
		fmt.Println("Listo.")
	}
	defer conn.Close()
	c := pb.NewBrokerClient(conn)

	scanner := bufio.NewScanner(os.Stdin);
	running:=true
	state = "IDLE"
	for running{
		fmt.Print(">")
		scanner.Scan()
		params:= strings.Split(scanner.Text(), " ")
		params[0]=strings.ToUpper(params[0])
		var r *pb.Reply
		err = nil
		n_value:="0"
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*300)
		switch params[0]{
			case "HELP":
				fmt.Println("Los siguientes comandos son reconocidos por Player:")
				fmt.Println("EXIT                      Cierra el programa Player")
				fmt.Println("HELP                      Muestra este mensaje")
				fmt.Println("ADDCITY [P] [C] <V>       Crea una entrada para la ciudad C en el planeta P.")
				fmt.Println("                          V indica la cantidad de rebeldes en C. Se guarda como 0 si se omite.")
				fmt.Println("DELETECITY [P] [C]        Elimina la entrada de la ciudad C en el planeta P.")
				fmt.Println("UPDATENAME [P] [C] [V]    Cambia el nombre de la ciudad C, en el planeta P, a V")
				fmt.Println("UPDATENUMBER [P] [C] [V]  Cambia la cantidad de rebeldes en la ciudad C, del planeta P, a V")
			case "EXIT":
				fmt.Println("Cerrando Informant.")
				running = false
			case "UPDATENAME":
				fallthrough
			case "UPDATENUMBER":
				if len(params)<4{	//UpdateName y UpdateNumber requieren 3 parámetros
					fmt.Println("El comando "+params[0]+" requiere 3 parámetros (Planeta, Ciudad, NuevoValor)")
					break
				}
				fallthrough
			case "ADDCITY":
				fallthrough
			case "DELETECITY":
				if len(params)<3{	//AddCity y DeleteCity requieren 2 parámetros. Se acepta un tercer parámetro
					fmt.Println("El comando "+params[0]+" requiere 2 parámetros (Planeta, Ciudad)")
					break
				}
				if len(params)==4{
					n_value=params[3]
				}
				r, err = c.GiveCommand(ctx, &pb.Command{Command:params[0],Planet:params[1],City:params[2],NewValue:n_value})
			default:
				fmt.Println("Comando desconocido. Use HELP para ver una lista de comandos.")
		}
		connectToFulcrum:=false
		r_addr:=""
		r_port:=""
		if r.GetReply()!=""{
			params:=strings.Split(r.GetReply(),";")
			for _, p := range params{
				pList := strings.Split(p,":")
				comm := pList[0]
				value := ""
				if len(pList)>1{
					value = pList[1]
				}
				debugLn("Response: ["+p+"]")
				switch comm{
					case "TELL":
						fmt.Println("Told "+value)
					case "COMM":
						fmt.Println("Command "+value)
					case "ADDR":
						if value[0]=='['{	//IPv6 address
							r_addr=p[strings.Index(p,"["):strings.Index(p,"]")+1]
							r_port=pList[len(pList)-1]
						} else {
							r_addr=value
							r_port=pList[2]
						}
						connectToFulcrum=true
				}
			}
		}
		if connectToFulcrum{
			debugLn("Conectando a Fulcrum ("+r_addr+":"+r_port+")")
			conn2, err := grpc.Dial(r_addr+":"+r_port, grpc.WithInsecure(), grpc.WithBlock())
			if err != nil {
				fmt.Printf("[Informant.main] Did not connect to Fulcrum: %v", err)
			} else {
				fmt.Println("Listo.")
			}
			fc := pb.NewFulcrumClient(conn2)
			ctx2, _ := context.WithTimeout(context.Background(), time.Second*300)
			r_value:=""
			switch params[0]{
				case "ADDCITY":
					debugLn("Sending ADDCITY command to Fulcrum.")
					reply, err := fc.AddCity(ctx2, &pb.Register{PlanetName: params[1],CityName:params[2],RebelCount:n_value,NewCityName:n_value})
					r_value=reply.GetReply()
					if err!=nil{
						fmt.Printf("ERROR: "+err.Error())
					}
				case "DELETECITY":
					reply, err := fc.DeleteCity(ctx2, &pb.Register{PlanetName: params[1],CityName:params[2],RebelCount:n_value,NewCityName:n_value})
					r_value=reply.GetReply()
					if err!=nil{
						fmt.Printf("ERROR: "+err.Error())
					}
				case "UPDATENAME":
					reply, err := fc.UpdateName(ctx2, &pb.Register{PlanetName: params[1],CityName:params[2],RebelCount:n_value,NewCityName:n_value})
					r_value=reply.GetReply()
					if err!=nil{
						fmt.Printf("ERROR: "+err.Error())
					}
				case "UPDATENUMBER":
					reply, err := fc.UpdateNumber(ctx2, &pb.Register{PlanetName: params[1],CityName:params[2],RebelCount:n_value,NewCityName:n_value})
					r_value=reply.GetReply()
					if err!=nil{
						fmt.Printf("ERROR: "+err.Error())
					}
				default:
					fmt.Println("The command given does not match any valid commands")
			}
			fmt.Println("Reply: "+r_value)
			conn2.Close()
		}
		if err != nil {
			fmt.Printf("[Informant.main] No se pudo enviar mensaje: %v", err)
		}
		cancel()
	}
}