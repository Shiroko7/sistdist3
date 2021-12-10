package main

import (
	"fmt"
	"context"
	"net"
	"google.golang.org/grpc"
	"io"
	"bufio"
	"os"
	"time"
	"strings"
	"strconv"
	pb "starwars"
)

type server struct {
	pb.UnimplementedFulcrumServer
}

var (
	file_clocks = make(map[string][]int)
	server_id = -1
)

const (
	port = ":60052"
	brokerAddress = "localhost:60051"
)

func updateClock(filename string) {
	if _, ok := file_clocks[filename]; ok {
		file_clocks[filename][server_id] += 1
	} else {
		new_clock := []int{0,0,0}
		new_clock[server_id] = 1 
		file_clocks[filename] = new_clock
	}
}

func registerOnLog(play string, replica_id string){
	fmt.Print("Registrando play en log... ")
	f, err := os.OpenFile(replica_id+".txt", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		fmt.Printf("No se pudo abrir archivo de log: %v\n", err)
	} 

	f.WriteString(play + "\n")
	if err != nil {
		fmt.Printf("No se pudo guardar archivo de texto: %v\n", err)
	} 

	err = f.Close()
	if err != nil {
		fmt.Printf("No se pudo cerrar archivo de texto: %v\n", err)
	} else {
		fmt.Println("Registro registrado correctamente.")
	}
}


func (s *server) AddCity(ctx context.Context, register *pb.Register) (*pb.Reply, error){
	fmt.Println("[AddCity] Agregando ciudad... ")
	f, err := os.OpenFile(register.PlanetName + ".txt", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		fmt.Printf("[AddCity] No se pudo abrir archivo del planeta: %v\n", err)
	} 

	f.WriteString(register.PlanetName + " " + register.CityName + " " + register.RebelCount + "\n")
	if err != nil {
		fmt.Printf("[AddCity] No se pudo guardar archivo de texto: %v\n", err)
	} 

	err = f.Close()
	if err != nil {
		fmt.Printf("[AddCity] No se pudo cerrar archivo de texto: %v\n", err)
	} else {
		fmt.Println("[AddCity] Registro registrado correctamente.")
	}
	registerOnLog("AddCity " + register.PlanetName + " " + register.CityName + " " + register.RebelCount, "asdf1")
	updateClock(register.PlanetName)
	clock:=file_clocks[register.PlanetName]
	r:=strconv.Itoa(clock[0])+","+strconv.Itoa(clock[1])+","+strconv.Itoa(clock[2])
	return &pb.Reply{Reply: "CLCK:"+r+";SERV:"+strconv.Itoa(server_id)}, nil
}

func (s *server) UpdateName(ctx context.Context, register *pb.Register) (*pb.Reply, error){
	fmt.Println("[UpdateName] Modificando ciudad... ")

	// reading
	f, err := os.OpenFile(register.PlanetName + ".txt", os.O_RDONLY, 0600)
	if err != nil {
		fmt.Printf("[UpdateName] No se pudo abrir archivo del planeta: %v\n", err)
	} 
	reader := bufio.NewReader(f)
	lines := ""
	line_split := []string{"", "", ""}
	found:=false
	for {
		line_i, err := reader.ReadString('\n')
		line_i=strings.Trim(line_i," \n\r")
		if err == io.EOF{
			if !found{
				fmt.Printf("[UpdateName] Registro no encontrado: %v\n", err)
			}
			break
		}
		if err != nil {
			fmt.Printf("[UpdateName] No se pudo leer linea del planeta: %v\n", err)
		} 
		line_split = strings.Split(line_i, " ")
		if len(line_split)!=3{
			fmt.Printf("[UpdateName] ERROR: Malformed entry ["+line_i+"]")
			continue
		}
		if line_split[1] == register.CityName {
			found=true
			line_i = line_split[0] +" "+ register.NewCityName +" "+ line_split[2]
		}
		lines = lines + line_i + "\n"
	}
	err = f.Close()
	if err != nil {
		fmt.Printf("[UpdateName] No se pudo cerrar archivo de texto: %v\n", err)
	}
	
	// modifying
	f, err = os.OpenFile(register.PlanetName + ".txt", os.O_WRONLY, 0600)	
	f.WriteString(lines)
	err = f.Close()
	if err != nil {
		fmt.Printf("[UpdateName] No se pudo cerrar archivo de texto: %v\n", err)
	} else {
		fmt.Println("[UpdateName] Registro registrado correctamente.")
	}

	registerOnLog("UpdateName " + register.PlanetName + " " + register.CityName + " " + register.NewCityName, "asdf1")
	updateClock(register.PlanetName)
	clock:=file_clocks[register.PlanetName]
	r:=strconv.Itoa(clock[0])+","+strconv.Itoa(clock[1])+","+strconv.Itoa(clock[2])
	return &pb.Reply{Reply: "CLCK:"+r+";SERV:"+strconv.Itoa(server_id)}, nil
}

func (s *server) UpdateNumber(ctx context.Context, register *pb.Register) (*pb.Reply, error){
	fmt.Println("[UpdateNumber] Modificando ciudad... ")

	// reading
	f, err := os.OpenFile(register.PlanetName + ".txt", os.O_RDONLY, 0600)
	if err != nil {
		fmt.Printf("[UpdateNumber] No se pudo abrir archivo del planeta: %v\n", err)
	} 
	reader := bufio.NewReader(f)
	lines := ""
	line_split := []string{"", "", ""}
	found:=false
	for {
		line_i, err := reader.ReadString('\n')
		line_i=strings.Trim(line_i," \n\r")
		if err == io.EOF{
			if !found{
				fmt.Printf("[UpdateNumber] Registro no encontrado: %v\n", err)
			}
			break
		}
		if err != nil {
			fmt.Printf("[UpdateNumber] No se pudo leer linea del planeta: %v\n", err)
		} 
		line_split = strings.Split(line_i, " ")
		if len(line_split)!=3{
			fmt.Printf("[UpdateNumber] ERROR: Malformed entry ["+line_i+"]")
			continue
		}
		if line_split[1] == register.CityName {
			found=true
			line_i = line_split[0] +" "+ line_split[1] +" "+ register.RebelCount
		}
		lines = lines + line_i + "\n"
	}
	err = f.Close()
	if err != nil {
		fmt.Printf("[UpdateNumber] No se pudo cerrar archivo de texto: %v\n", err)
	}
	
	// modifying
	f, err = os.OpenFile(register.PlanetName + ".txt", os.O_WRONLY, 0600)	
	f.WriteString(lines)
	err = f.Close()
	if err != nil {
		fmt.Printf("[UpdateNumber] No se pudo cerrar archivo de texto: %v\n", err)
	} else {
		fmt.Println("[UpdateNumber] Registro registrado correctamente.")
	}

	registerOnLog("UpdateNumber " + register.PlanetName + " " + register.CityName + " " + register.RebelCount, "asdf1")
	updateClock(register.PlanetName)
	clock:=file_clocks[register.PlanetName]
	r:=strconv.Itoa(clock[0])+","+strconv.Itoa(clock[1])+","+strconv.Itoa(clock[2])
	return &pb.Reply{Reply: "CLCK:"+r+";SERV:"+strconv.Itoa(server_id)}, nil
}

func (s *server) DeleteCity(ctx context.Context, register *pb.Register) (*pb.Reply, error){
	fmt.Println("[DeleteCity] Modificando ciudad... ")

	// reading
	f, err := os.OpenFile(register.PlanetName + ".txt", os.O_RDONLY, 0600)
	if err != nil {
		fmt.Printf("[DeleteCity] No se pudo abrir archivo del planeta: %v\n", err)
	} 
	reader := bufio.NewReader(f)
	lines := ""
	line_split := []string{"", "", ""}
	found:=false
	for {
		line_i, err := reader.ReadString('\n')
		line_i=strings.Trim(line_i," \n\r")
		if err == io.EOF{
			if !found{
				fmt.Printf("[DeleteCity] Registro no encontrado: %v\n", err)
			}
			break
		}
		if err != nil {
			fmt.Printf("[DeleteCity] No se pudo leer linea del planeta: %v\n", err)
		} 
		line_split = strings.Split(line_i, " ")
		if len(line_split)!=3{
			fmt.Printf("[DeleteCity] ERROR: Malformed entry ["+line_i+"]")
			continue
		}
		if line_split[1] != register.CityName {
			lines = lines + line_i + "\n"
		} else {
			found=true
		}
	}
	err = f.Close()
	if err != nil {
		fmt.Printf("[DeleteCity] No se pudo cerrar archivo de texto: %v\n", err)
	}
	
	// modifying
	f, err = os.OpenFile(register.PlanetName + ".txt", os.O_WRONLY, 0600)	
	f.WriteString(lines)
	err = f.Close()
	if err != nil {
		fmt.Printf("[DeleteCity] No se pudo cerrar archivo de texto: %v\n", err)
	} else {
		fmt.Println("[DeleteCity] Registro registrado correctamente.")
	}

	registerOnLog("DeleteCity " + register.PlanetName + " " + register.CityName, "asdf1")
	updateClock(register.PlanetName)
	clock:=file_clocks[register.PlanetName]
	r:=strconv.Itoa(clock[0])+","+strconv.Itoa(clock[1])+","+strconv.Itoa(clock[2])
	return &pb.Reply{Reply: "CLCK:"+r+";SERV:"+strconv.Itoa(server_id)}, nil
}

func (s *server) RequestRebels(ctx context.Context, request *pb.RequestRebel) (*pb.Reply, error){
	fmt.Println("[RequestRebels] Buscando rebels de: " + request.CityName)
	f, err := os.OpenFile(request.PlanetName + ".txt", os.O_RDONLY, 0600)
	if err != nil {
		fmt.Printf("[RequestRebels] No se pudo abrir archivo del planeta: %v\n", err)
	} 
	reader := bufio.NewReader(f)
	value := "-1"
	line_split := []string{"", "", ""}
	found:=false
	for {
		line_i, err := reader.ReadString('\n')
		line_i=strings.Trim(line_i," \n\r")
		if err == io.EOF{
			if !found{
				fmt.Printf("[RequestRebels] Registro no encontrado: %v\n", err)
			}
			break
		}
		if err != nil {
			fmt.Printf("[RequestRebels] No se pudo leer linea del log: %v\n", err)
		} 
		line_split = strings.Split(line_i, " ")
		if len(line_split)!=3{
			fmt.Printf("[RequestRebels] ERROR: Malformed entry ["+line_i+"]")
			continue
		}
		if line_split[1] == request.CityName {
			found=true
			fmt.Println("[RequestRebels] Entry found in planet file: ["+line_i+"]")
			value = line_split[2]
			break
		}
	}
	err = f.Close()
	if err != nil {
		fmt.Printf("[RequestRebels] No se pudo cerrar archivo de texto: %v\n", err)
	//} else {
	//	fmt.Println("Jugada registrada correctamente.")
	}
	updateClock(request.PlanetName)
	clock:=file_clocks[request.PlanetName]
	r:=strconv.Itoa(clock[0])+","+strconv.Itoa(clock[1])+","+strconv.Itoa(clock[2])
	return &pb.Reply{Reply: "CLCK:"+r+";SERV:"+strconv.Itoa(server_id)+";RVAL:"+value}, nil
}

//Se conecta a Broker y se reporta este servidor Fulcrum
func connectBroker(){
	fmt.Println("Iniciando Fulcrum Server");
	// Set up a connection to the server.
	fmt.Println("Conectando a Broker... ");
	conn, err := grpc.Dial(brokerAddress, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		fmt.Printf("Did not connect: %v", err)
	} else {
		fmt.Println("Listo.")
	}
	defer conn.Close()
	c := pb.NewBrokerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*300)
	reply, _ := c.ReportFulcrum(ctx, &pb.None{})
	server_id, _ = strconv.Atoi(reply.Reply)
	fmt.Println("Asignada ID "+strconv.Itoa(server_id));
	cancel()
}

func listenRequests(port string) {
	fmt.Print("Abriendo puerto "+port+"... ");
	lis, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Printf("No se pudo realizar: %v\n", err)
	} else {
		fmt.Println("Listo.")
	}
	s := grpc.NewServer()
	pb.RegisterFulcrumServer(s, &server{})
	if err := s.Serve(lis); err != nil {
		fmt.Printf("No se pudo servir: %v\n", err)
	}
}

func main(){
	connectBroker()
	listenRequests(port)
	/*
	fmt.Println(file_clocks)
	updateClock("asdf")
	fmt.Println(file_clocks)
	updateClock("asdf")
	fmt.Println(file_clocks)
	*/
}