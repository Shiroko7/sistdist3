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
	state = "WAKE"						//Estado actual. Se usa para control de flujo
	fulcrums [3]string					//Direcciones de los tres servidores FULCRUM
	reportedFulcrums = 0
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
func (s *server) ReportFulcrum(ctx context.Context, in *pb.FulcrumID) (*pb.Reply, error){
	p, _ := peer.FromContext(ctx)
	a:=p.Addr.String()[:strings.LastIndex(p.Addr.String(), ":")]	//IP Address
	id, _:= strconv.Atoi(in.GetID())
	if fulcrums[id] == "" {
		fulcrums[id] = a+fulcrumPort
		fmt.Println("Fulcrum "+in.GetID()+" registrado: " + a+fulcrumPort)
		reportedFulcrums++;
	} else {
		fmt.Println("Fulcrum se intento comunicar, pero ya se ha registrado un Fulcrum con el ID indicado.")
		return &pb.Reply{Reply: "FULL"}, nil
	}
	return &pb.Reply{Reply: "DONE:"+in.GetID()}, nil
}

//GIVECOMMAND
func (s *server) GiveCommand(ctx context.Context, in *pb.Command) (*pb.Reply, error){
	debugLn("RECV GiveCommand")
	debugLn("[Command  : "+in.GetCommand()+"]")
	debugLn("[Planet   : "+in.GetPlanet()+"]")
	debugLn("[City     : "+in.GetCity()+"]")
	debugLn("[New Value: "+in.GetNewValue()+"]")
	//Get address of random Broker
	rPort:=fulcrums[rand.Intn(3)]
	//Return it
	
	//ctx2, cancel := context.WithTimeout(context.Background(), time.Second*300)
	//r, err := c.RequestPool(ctx2, &pb.None{})
	//cancel()
	return &pb.Reply{Reply: "ADDR:"+rPort}, nil
}

//REQUESTREBELS
func (s *server) RequestRebels(ctx context.Context, in *pb.RequestRebel) (*pb.Reply, error){
	// Set up a connection to a random Fulcrum.
	rF:=fulcrums[rand.Intn(2)]
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

func (s *server) RequestFulcrums(ctx context.Context, in *pb.None) (*pb.Fulcrums, error){
	wait:=true
	for wait{
		if reportedFulcrums<2{
			time.Sleep(time.Second * 5)
		} else {
			wait = false
		}
	}
	return &pb.Fulcrums{F0:fulcrums[0],F1:fulcrums[1],F2:fulcrums[2]}, nil
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
