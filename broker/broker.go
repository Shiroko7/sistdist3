package main

import (
	"fmt"
	"bufio"
	"context"
	"time"
	"net"
	"math/rand"
	"strings"
/*
	"os"
	"strconv"
*/
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	pb "starwars"
)

const (
	port = ":60051"						//Puerto abierto por BROKER
	fulcrumPort = ":60052"				//Puerto abierto por cada FULCRUM para conexiones
	debugging = true					//Indica si se deben imprimir mensajes de debugging
)

var (
	running = true						//Es TRUE mientras Broker deba mantenerse corriendo
	scanner *bufio.Scanner				//Scanner para leer input desde consola
	//c pb.PoolClient						//Cliente de Pool
	state = "WAKE"						//Estado actual. Se usa para control de flujo
	fulcrums [3]string					//Direcciones de los tres servidores FULCRUM
)

//Valor absoluto.
func abs(i int) int{
	if i<0{i=-i}
	return i
}

//Imprime mensajes de debugging
func debug(s string){
	if debugging{fmt.Print(s)}
}

//Imprime mensajes de debugging
func debugLn(s string){
	if debugging{
		fmt.Println(s)
	}
}

type server struct {
	pb.UnimplementedBrokerServer
}

//Llamado por FULCRUM. Indica que el servidor está levantado y listo para conectarse.
//Se retorna el puerto
func (s *server) ReportFulcrum(ctx context.Context, in *pb.None) (*pb.Reply, error){
	id:="-1"
	p, _ := peer.FromContext(ctx)
	a:=p.Addr.String()[:strings.LastIndex(p.Addr.String(), ":")]	//IP Address
	if fulcrums[0] == "" {
		fulcrums[0] = a+fulcrumPort
		id="0"
		fmt.Println("Fulcrum 1 registrado: " + a+fulcrumPort)
	/*
	} else if fulcrums[1] == "" {
		fulcrums[1] = a+fulcrumPort
		id="1"
		fmt.Println("Fulcrum 2  registrado: " + a+fulcrumPort)
	} else if fulcrums[2] == "" {
		fulcrums[2] = a+fulcrumPort
		id="2"
		fmt.Println("Fulcrum 3 registrado: " + a+fulcrumPort)
	*/
	} else {
		fmt.Println("Fulcrum se intento comunicar, pero ya estan definidos todos los puertos.")
		return &pb.Reply{Reply: "FULL"}, nil
	}
	return &pb.Reply{Reply: "DONE:"+id}, nil
}

//GIVECOMMAND
func (s *server) GiveCommand(ctx context.Context, in *pb.Command) (*pb.Reply, error){
	debugLn("RECV GiveCommand")
	debugLn("[Command  : "+in.GetCommand()+"]")
	debugLn("[Planet   : "+in.GetPlanet()+"]")
	debugLn("[City     : "+in.GetCity()+"]")
	debugLn("[New Value: "+in.GetNewValue()+"]")
	//Get address of random Broker
	rPort:=fulcrums[0]//rand.Intn(2)]
	//Return it
	
	//ctx2, cancel := context.WithTimeout(context.Background(), time.Second*300)
	//r, err := c.RequestPool(ctx2, &pb.None{})
	//cancel()
	return &pb.Reply{Reply: "ADDR:"+rPort}, nil
}

//REQUESTREBELS
func (s *server) RequestRebels(ctx context.Context, in *pb.RequestRebel) (*pb.Reply, error){
	// Set up a connection to a random Fulcrum.
	rF:=fulcrums[0]//rand.Intn(2)]
	debug("Conectando a Fulcrum aleatorio ("+rF+")... ");
	conn, err := grpc.Dial(rF, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		fmt.Printf("[Broker.RequestRebels] No se pudo realizar: %v", err)
	} else {
		fmt.Println("Listo.")
	}
	defer conn.Close()
	ctx2, _ := context.WithTimeout(context.Background(), time.Second*300)
	c := pb.NewFulcrumClient(conn)
	reply, err := c.RequestRebels(ctx2, &pb.RequestRebel{PlanetName: in.GetPlanetName(), CityName: in.GetCityName()})
	if err!=nil{
		fmt.Printf("ERROR: "+err.Error())
	}
	return reply, err
}

func main(){
	fmt.Println("Iniciando Broker");
	rand.Seed(time.Now().UnixNano())
	fmt.Print("Abriendo puerto "+port+"... ");
	lis, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Printf("[Broker.main] No se pudo realizar: %v", err)
	} else {
		fmt.Println("Listo.")
	}
	s := grpc.NewServer()
	pb.RegisterBrokerServer(s, &server{})
	fmt.Println("Servidor Broker listo.");
	s.Serve(lis)
}
