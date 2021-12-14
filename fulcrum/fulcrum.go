package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"google.golang.org/grpc"
	"io"
	"net"
	"os"
	"path/filepath"
	pb "starwars"
	"strconv"
	"strings"
	"time"
)

type server struct {
	pb.UnimplementedFulcrumServer
}

var (
	file_clocks = make(map[string][]int)
	replica_id  = -1
	fulcrum_addresses [3]string
)

const (
	port     = ":60052"
	brokerAddress = "dist93:60051"
)

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func updateClock(filename string) {
	if _, ok := file_clocks[filename]; ok {
		file_clocks[filename][replica_id] += 1
	} else {
		new_clock := []int{0, 0, 0}
		new_clock[replica_id] = 1
		file_clocks[filename] = new_clock
	}
}

func registerOnLog(play string) {
	fmt.Print("Registrando play en log... ")
	f, err := os.OpenFile(strconv.Itoa(replica_id)+".txt", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
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

func readLog(replica_id string) ([]string, error) {
	var lines []string
	if fileExists(replica_id + ".txt") {
		fmt.Println("Leyendo log en replica " + replica_id + " ...")
		f, err := os.OpenFile(replica_id+".txt", os.O_RDONLY, 0600)
		if err != nil {
			fmt.Printf("No se pudo abrir archivo de log: %v\n", err)
		}

		reader := bufio.NewReader(f)

		for {
			line_i, err := reader.ReadString('\n')
			line_i = strings.Trim(line_i, " \n\r")
			if err == io.EOF {
				break
			}
			if err != nil {
				fmt.Printf("[readLog] No se pudo leer linea del log: %v\n", err)
			} else {
				lines = append(lines, line_i)
			}
		}

		err = f.Close()
		if err != nil {
			fmt.Printf("No se pudo cerrar archivo de texto: %v\n", err)
		} else {
			fmt.Println("Log leido correctamente.")
		}
	}
	return lines, nil
}

func addCityOnFile(planet_name string, city_name string, rebel_count string) {
	f, err := os.OpenFile("planets/"+planet_name+".txt", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		fmt.Printf("[AddCity] No se pudo abrir archivo del planeta: %v\n", err)
	}

	f.WriteString(planet_name + " " + city_name + " " + rebel_count + "\n")
	if err != nil {
		fmt.Printf("[AddCity] No se pudo guardar archivo de texto: %v\n", err)
	}

	err = f.Close()
	if err != nil {
		fmt.Printf("[AddCity] No se pudo cerrar archivo de texto: %v\n", err)
	} else {
		fmt.Println("[AddCity] Registro registrado correctamente.")
	}
}

func (s *server) AddCity(ctx context.Context, register *pb.Register) (*pb.Reply, error) {
	fmt.Println("[AddCity] Agregando ciudad... ")
	if !fileExists("planets/" + register.PlanetName + ".txt") {
		os.OpenFile("planets/"+register.PlanetName+".txt", os.O_RDONLY|os.O_CREATE, 0666)
	}
	addCityOnFile(register.PlanetName, register.CityName, register.RebelCount)
	if replica_id!=0{
		fmt.Println("Registered on puppet; sending to master ["+fulcrum_addresses[0]+"]")
		conn, _ := grpc.Dial(fulcrum_addresses[0], grpc.WithInsecure(), grpc.WithBlock())
		c := pb.NewFulcrumClient(conn)
		ctx2, cancel := context.WithTimeout(context.Background(), time.Second*300)
		c.AddCity(ctx2, register)
		cancel()
		conn.Close()
	}
	registerOnLog("AddCity " + register.PlanetName + " " + register.CityName + " " + register.RebelCount)
	updateClock(register.PlanetName)
	clock := file_clocks[register.PlanetName]
	r := strconv.Itoa(clock[0]) + "," + strconv.Itoa(clock[1]) + "," + strconv.Itoa(clock[2])
	return &pb.Reply{Reply: "CLCK:" + r + ";SERV:" + strconv.Itoa(replica_id)}, nil
}

func updateNameOnFile(planet_name string, city_name string, new_city_name string) {
	// reading
	f, err := os.OpenFile("planets/"+planet_name+".txt", os.O_RDONLY, 0600)
	if err != nil {
		fmt.Printf("[UpdateName] No se pudo abrir archivo del planeta: %v\n", err)
	}
	reader := bufio.NewReader(f)
	lines := ""
	line_split := []string{"", "", ""}
	found := false
	for {
		line_i, err := reader.ReadString('\n')
		line_i = strings.Trim(line_i, " \n\r")
		if err == io.EOF {
			if !found {
				fmt.Printf("[UpdateName] Registro no encontrado: %v\n", err)
			}
			break
		}
		if err != nil {
			fmt.Printf("[UpdateName] No se pudo leer linea del planeta: %v\n", err)
		}
		line_split = strings.Split(line_i, " ")
		if len(line_split) != 3 {
			fmt.Printf("[UpdateName] ERROR: Malformed entry [" + line_i + "]")
			continue
		}
		if line_split[1] == city_name {
			found = true
			line_i = line_split[0] + " " + new_city_name + " " + line_split[2]
		}
		lines = lines + line_i + "\n"
	}
	err = f.Close()
	if err != nil {
		fmt.Printf("[UpdateName] No se pudo cerrar archivo de texto: %v\n", err)
	}

	// modifying
	f, err = os.OpenFile("planets/"+planet_name+".txt", os.O_WRONLY, 0600)
	f.WriteString(lines)
	err = f.Close()
	if err != nil {
		fmt.Printf("[UpdateName] No se pudo cerrar archivo de texto: %v\n", err)
	} else {
		fmt.Println("[UpdateName] Registro registrado correctamente.")
	}
}

func (s *server) UpdateName(ctx context.Context, register *pb.Register) (*pb.Reply, error) {
	fmt.Println("[UpdateName] Modificando ciudad... ")
	registerOnLog("UpdateName " + register.PlanetName + " " + register.CityName + " " + register.NewCityName)
	updateClock(register.PlanetName)
	clock := file_clocks[register.PlanetName]
	r := strconv.Itoa(clock[0]) + "," + strconv.Itoa(clock[1]) + "," + strconv.Itoa(clock[2])
	return &pb.Reply{Reply: "CLCK:" + r + ";SERV:" + strconv.Itoa(replica_id)}, nil
}

func updateNumberOnFile(planet_name string, city_name string, rebel_count string) {
	// reading
	f, err := os.OpenFile("planets/"+planet_name+".txt", os.O_RDONLY, 0600)
	if err != nil {
		fmt.Printf("[UpdateNumber] No se pudo abrir archivo del planeta: %v\n", err)
	}
	reader := bufio.NewReader(f)
	lines := ""
	line_split := []string{"", "", ""}
	found := false
	for {
		line_i, err := reader.ReadString('\n')
		line_i = strings.Trim(line_i, " \n\r")
		if err == io.EOF {
			if !found {
				fmt.Printf("[UpdateNumber] Registro no encontrado: %v\n", err)
			}
			break
		}
		if err != nil {
			fmt.Printf("[UpdateNumber] No se pudo leer linea del planeta: %v\n", err)
		}
		line_split = strings.Split(line_i, " ")
		if len(line_split) != 3 {
			fmt.Printf("[UpdateNumber] ERROR: Malformed entry [" + line_i + "]")
			continue
		}
		if line_split[1] == city_name {
			found = true
			line_i = line_split[0] + " " + line_split[1] + " " + rebel_count
		}
		lines = lines + line_i + "\n"
	}
	err = f.Close()
	if err != nil {
		fmt.Printf("[UpdateNumber] No se pudo cerrar archivo de texto: %v\n", err)
	}

	// modifying
	f, err = os.OpenFile("planets/"+planet_name+".txt", os.O_WRONLY, 0600)
	f.WriteString(lines)
	err = f.Close()
	if err != nil {
		fmt.Printf("[UpdateNumber] No se pudo cerrar archivo de texto: %v\n", err)
	} else {
		fmt.Println("[UpdateNumber] Registro registrado correctamente.")
	}
}

func (s *server) UpdateNumber(ctx context.Context, register *pb.Register) (*pb.Reply, error) {
	fmt.Println("[UpdateNumber] Modificando ciudad... ")
	registerOnLog("UpdateNumber " + register.PlanetName + " " + register.CityName + " " + register.RebelCount)
	updateClock(register.PlanetName)
	clock := file_clocks[register.PlanetName]
	r := strconv.Itoa(clock[0]) + "," + strconv.Itoa(clock[1]) + "," + strconv.Itoa(clock[2])
	return &pb.Reply{Reply: "CLCK:" + r + ";SERV:" + strconv.Itoa(replica_id)}, nil
}

func deleteCityOnFile(planet_name string, city_name string) {
	// reading
	f, err := os.OpenFile("planets/"+planet_name+".txt", os.O_RDONLY, 0600)
	if err != nil {
		fmt.Printf("[DeleteCity] No se pudo abrir archivo del planeta: %v\n", err)
	}
	reader := bufio.NewReader(f)
	lines := ""
	line_split := []string{"", "", ""}
	found := false
	for {
		line_i, err := reader.ReadString('\n')
		line_i = strings.Trim(line_i, " \n\r")
		if err == io.EOF {
			if !found {
				fmt.Printf("[DeleteCity] Registro no encontrado: %v\n", err)
			}
			break
		}
		if err != nil {
			fmt.Printf("[DeleteCity] No se pudo leer linea del planeta: %v\n", err)
		}
		line_split = strings.Split(line_i, " ")
		if len(line_split) != 3 {
			fmt.Printf("[DeleteCity] ERROR: Malformed entry [" + line_i + "]")
			continue
		}
		if line_split[1] != city_name {
			lines = lines + line_i + "\n"
		} else {
			found = true
		}
	}
	err = f.Close()
	if err != nil {
		fmt.Printf("[DeleteCity] No se pudo cerrar archivo de texto: %v\n", err)
	}

	// modifying
	f, err = os.OpenFile("planets/"+planet_name+".txt", os.O_WRONLY, 0600)
	f.WriteString(lines)
	err = f.Close()
	if err != nil {
		fmt.Printf("[DeleteCity] No se pudo cerrar archivo de texto: %v\n", err)
	} else {
		fmt.Println("[DeleteCity] Registro registrado correctamente.")
	}
}

func (s *server) DeleteCity(ctx context.Context, register *pb.Register) (*pb.Reply, error) {
	fmt.Println("[DeleteCity] Modificando ciudad... ")
	registerOnLog("DeleteCity " + register.PlanetName + " " + register.CityName)
	updateClock(register.PlanetName)
	clock := file_clocks[register.PlanetName]
	r := strconv.Itoa(clock[0]) + "," + strconv.Itoa(clock[1]) + "," + strconv.Itoa(clock[2])
	return &pb.Reply{Reply: "CLCK:" + r + ";SERV:" + strconv.Itoa(replica_id)}, nil
}

func (s *server) RequestRebels(ctx context.Context, request *pb.RequestRebel) (*pb.Reply, error) {
	fmt.Println("[RequestRebels] Buscando rebels de: " + request.CityName)
	f, err := os.OpenFile("planets/"+request.PlanetName+".txt", os.O_RDONLY, 0600)
	if err != nil {
		fmt.Printf("[RequestRebels] No se pudo abrir archivo del planeta: %v\n", err)
		return nil, err
	}
	reader := bufio.NewReader(f)
	value := "-1"
	line_split := []string{"", "", ""}
	found := false
	for {
		line_i, err := reader.ReadString('\n')
		line_i = strings.Trim(line_i, " \n\r")
		if err == io.EOF {
			if !found {
				fmt.Printf("[RequestRebels] Registro no encontrado: %v\n", err)
			}
			break
		}
		if err != nil {
			fmt.Printf("[RequestRebels] No se pudo leer linea del log: %v\n", err)
		}
		line_split = strings.Split(line_i, " ")
		if len(line_split) != 3 {
			fmt.Printf("[RequestRebels] ERROR: Malformed entry [" + line_i + "]")
			continue
		}
		if line_split[1] == request.CityName {
			found = true
			fmt.Println("[RequestRebels] Entry found in planet file: [" + line_i + "]")
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
	clock := file_clocks[request.PlanetName]
	r := strconv.Itoa(clock[0]) + "," + strconv.Itoa(clock[1]) + "," + strconv.Itoa(clock[2])
	return &pb.Reply{Reply: "CLCK:" + r + ";SERV:" + strconv.Itoa(replica_id) + ";RVAL:" + value}, nil
}

func (s *server) ReportChanges(ctx context.Context, in *pb.None) (*pb.Changes, error) {
	log, _ := readLog(strconv.Itoa(replica_id))
	changes_list := strings.Join(log, "-")
	b_clock, _ := json.Marshal(file_clocks)
	return &pb.Changes{Log: changes_list, Clock: b_clock}, nil
}

func (s *server) RecieveNewClock(ctx context.Context, in *pb.Changes) (*pb.None, error) {
	json.Unmarshal(in.Clock, &file_clocks)
	fmt.Println("New clocks:", file_clocks)
	return &pb.None{}, nil
}

func sendNewClocks(port string) {
	// Set up a connection to the server.
	fmt.Println("Conectando a fulcrum " + port)
	conn, err := grpc.Dial(port, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		fmt.Printf("Did not connect: %v", err)
	} else {
		fmt.Println("Listo.")
	}
	defer conn.Close()
	c := pb.NewFulcrumClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*300)
	b_clock, _ := json.Marshal(file_clocks)
	c.RecieveNewClock(ctx, &pb.Changes{Log: "", Clock: b_clock})
	cancel()
	conn.Close()
}

//Se conecta a Broker y se reporta este servidor Fulcrum
func connectBroker() {
	// Set up a connection to the server.
	fmt.Println("Conectando a Broker... ")
	conn, err := grpc.Dial(brokerAddress, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		fmt.Printf("Did not connect: %v", err)
	} else {
		fmt.Println("Listo.")
	}
	defer conn.Close()
	c := pb.NewBrokerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*300)
	c.ReportFulcrum(ctx, &pb.FulcrumID{ID:strconv.Itoa(replica_id)})
	cancel()
	ctx, cancel = context.WithTimeout(context.Background(), time.Second*300)
	reply, _ := c.RequestFulcrums(ctx, &pb.None{})
	fulcrum_addresses[0]=reply.GetF0()
	fulcrum_addresses[1]=reply.GetF1()
	fulcrum_addresses[2]=reply.GetF2()
	cancel()
}

func askFulcrumChanges(address string) (log string, clock []byte) {
	// Set up a connection to the server.
	fmt.Println("Conectando a fulcrum " + address)
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		fmt.Printf("Did not connect: %v", err)
	} else {
		fmt.Println("Listo.")
	}
	defer conn.Close()
	c := pb.NewFulcrumClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*300)
	reply, _ := c.ReportChanges(ctx, &pb.None{})
	cancel()
	conn.Close()
	return reply.Log, reply.Clock
}

func listenRequests(port string) {
	fmt.Print("Abriendo puerto " + port + "... ")
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

func propagateChanges() {
	for range time.Tick(time.Second * 120) {
		fmt.Println("Propagating changes")
		//replica 2
		log2, clock2 := askFulcrumChanges(fulcrum_addresses[2])
		log_list_2 := strings.Split(log2, "-")
		var file_clocks_2 map[string][]int
		err := json.Unmarshal(clock2, &file_clocks_2)
		if err != nil {
			fmt.Println("No se pudo unmarshal el reloj 2")
		}
		//replica 1
		log1, clock1 := askFulcrumChanges(fulcrum_addresses[2])
		log_list_1 := strings.Split(log1, "-")
		var file_clocks_1 map[string][]int
		err = json.Unmarshal(clock1, &file_clocks_1)
		if err != nil {
			fmt.Println("No se pudo unmarshal el reloj 1")
		}

		// replica 0 (master)
		log_list, _ := readLog("0")

		var change_split []string
		planet := ""
		// look for every planet
		err = filepath.Walk("planets/", func(path string, info os.FileInfo, err error) error {
			if err != nil {
				fmt.Println(err)
				return err
			}
			if !info.IsDir() {
				planet = strings.Split(path, "/")[1]
				planet = planet[0 : len(planet)-4]
				if _, ok := file_clocks_2[planet]; !ok {
					file_clocks_2[planet] = []int{0, 0, 0}
				}
				if _, ok := file_clocks_1[planet]; !ok {
					file_clocks_1[planet] = []int{0, 0, 0}
				}
				if _, ok := file_clocks[planet]; !ok {
					file_clocks[planet] = []int{0, 0, 0}
				}
				// changes in puppet 2
				// if any changes
				if file_clocks_2[planet][1] > file_clocks[planet][1] {
					// add them
					for _, change := range log_list_2 {
						change_split = strings.Split(change, " ")
						if change_split[1] == planet {
							if change_split[0] == "AddCity" {
								addCityOnFile(change_split[1], change_split[2], change_split[3])
							} else if change_split[0] == "UpdateName" {
								updateNameOnFile(change_split[1], change_split[2], change_split[3])
							} else if change_split[0] == "UpdateNumber" {
								updateNumberOnFile(change_split[1], change_split[2], change_split[3])
							} else if change_split[0] == "DeleteCity" {
								deleteCityOnFile(change_split[1], change_split[2])
							}
						}
					}
					file_clocks[planet][1] = file_clocks_2[planet][1]
				}
				// changes in puppet 1
				// if any changes
				if file_clocks_1[planet][1] > file_clocks[planet][1] {
					// add them
					for _, change := range log_list_1 {
						change_split = strings.Split(change, " ")
						if change_split[1] == planet {
							if change_split[0] == "AddCity" {
								addCityOnFile(change_split[1], change_split[2], change_split[3])
							} else if change_split[0] == "UpdateName" {
								updateNameOnFile(change_split[1], change_split[2], change_split[3])
							} else if change_split[0] == "UpdateNumber" {
								updateNumberOnFile(change_split[1], change_split[2], change_split[3])
							} else if change_split[0] == "DeleteCity" {
								deleteCityOnFile(change_split[1], change_split[2])
							}
						}
					}
					file_clocks[planet][1] = file_clocks_1[planet][1]
				}

				// changes in master
				// if any changes
				if file_clocks[planet][0] > file_clocks_1[planet][0] {
					// add them
					for _, change := range log_list {
						change_split = strings.Split(change, " ")
						if change_split[1] == planet {
							if change_split[0] == "AddCity" {
								addCityOnFile(change_split[1], change_split[2], change_split[3])
							} else if change_split[0] == "UpdateName" {
								updateNameOnFile(change_split[1], change_split[2], change_split[3])
							} else if change_split[0] == "UpdateNumber" {
								updateNumberOnFile(change_split[1], change_split[2], change_split[3])
							} else if change_split[0] == "DeleteCity" {
								deleteCityOnFile(change_split[1], change_split[2])
							}
						}
					}
				}
			}
			return nil
		})
		if err != nil {
			fmt.Println(err)
		}
		// empty logs
		os.Remove("0.txt")
		os.Remove("1.txt")
		os.Remove("2.txt")
		// send new clocks
		sendNewClocks(fulcrum_addresses[1])
		sendNewClocks(fulcrum_addresses[2])
	}
}

func main() {
	fmt.Println("Iniciando Fulcrum Server...")
	replica_id, _ = strconv.Atoi(os.Args[1])
	connectBroker()

	if replica_id == 0 {
		fmt.Println("Master Node ["+port+"]")
		go propagateChanges()
		listenRequests(port)
	} else {
		fmt.Println("Puppet Node ["+port+"]")
		listenRequests(port)
	}
}
