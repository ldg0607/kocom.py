#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
python kocom script (Fixed & Optimized Version)
"""

import configparser
import json
import logging
import os
import platform
import queue
import random
import select
import socket
import sys
import threading
import time

try:
    import serial
except ImportError:
    serial = None

import paho.mqtt.client as mqtt

# define -------------------------------
SW_VERSION = "2026.01.17"
CONFIG_FILE = "kocom.conf"
BUF_SIZE = 100

read_write_gap = 0.05  # [튜닝] 통신 안정성을 위해 간격 조정
polling_interval = 300
elevator_timer = None

header_h = "aa55"
trailer_h = "0d0d"
packet_size = 21
chksum_position = 18

type_t_dic = {"30b": "send", "30d": "ack"}
seq_t_dic = {"c": 1, "d": 2, "e": 3, "f": 4}
device_t_dic = {
    "01": "wallpad",
    "0e": "light",
    "2c": "gas",
    "36": "thermo",
    "3b": "plug",
    "44": "elevator",
    "48": "fan",
}
cmd_t_dic = {"00": "state", "01": "on", "02": "off", "3a": "query"}
room_t_dic = {"00": "livingroom", "01": "bedroom", "02": "room1", "03": "room2"}

type_h_dic = {v: k for k, v in type_t_dic.items()}
seq_h_dic = {v: k for k, v in seq_t_dic.items()}
device_h_dic = {v: k for k, v in device_t_dic.items()}
cmd_h_dic = {v: k for k, v in cmd_t_dic.items()}
room_h_dic = {
    "livingroom": "00",
    "myhome": "00",
    "bedroom": "01",
    "room1": "02",
    "room2": "03",
}

device_socket_map = {}

# mqtt functions ----------------------------


def init_mqttc():
    # Paho MQTT 버전 호환성 체크 (v1.x / v2.x)
    try:
        # Paho v2.x
        mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    except AttributeError:
        # Paho v1.x
        logging.info("[MQTT] Detected Paho MQTT v1.x")
        mqttc = mqtt.Client()

    mqttc.on_message = mqtt_on_message
    mqttc.on_subscribe = mqtt_on_subscribe
    mqttc.on_connect = mqtt_on_connect
    mqttc.on_disconnect = mqtt_on_disconnect

    if config.get("MQTT", "mqtt_allow_anonymous", fallback="False") != "True":
        logtxt = "[MQTT] connecting (using username and password)"
        mqttc.username_pw_set(
            username=config.get("MQTT", "mqtt_username", fallback=""),
            password=config.get("MQTT", "mqtt_password", fallback=""),
        )
    else:
        logtxt = "[MQTT] connecting (anonymous)"

    mqtt_server = config.get("MQTT", "mqtt_server")
    mqtt_port = int(config.get("MQTT", "mqtt_port"))

    for retry_cnt in range(1, 31):
        try:
            logging.info(logtxt)
            mqttc.connect(mqtt_server, mqtt_port, 60)
            mqttc.loop_start()
            return mqttc
        except Exception as e:
            logging.error(f"[MQTT] connection failure #{retry_cnt}: {e}")
            time.sleep(10)
    return False


def mqtt_on_subscribe(mqttc, obj, mid, reason_code_list, properties=None):
    logging.info(f"[MQTT] Subscribed: {mid}")


def mqtt_on_log(mqttc, obj, level, string):
    logging.info("[MQTT] on_log : " + string)


def mqtt_on_connect(mqttc, userdata, flags, reason_code, properties=None):
    # Paho v1/v2 호환성을 위해 properties=None 처리
    rc = reason_code.value if hasattr(reason_code, "value") else reason_code
    if rc == 0:
        logging.info("[MQTT] Connected - 0: OK")
        mqttc.subscribe("kocom/#", 0)
    else:
        logging.error(f"[MQTT] Connection error - {rc}")


def mqtt_on_disconnect(
    mqttc, userdata, disconnect_flags, reason_code=0, properties=None
):
    logging.error(f"[MQTT] Disconnected - {reason_code}")


# serial/socket communication class & functions--------------------


class RS485Wrapper:
    def __init__(
        self,
        serial_port=None,
        socket_server=None,
        socket_port=0,
        socket_server2=None,
        socket_port2=0,
    ):
        if socket_server is None:
            self.type = "serial"
            self.serial_port = serial_port
        else:
            self.type = "socket"
            self.socket_servers = [(socket_server, socket_port)]
            if socket_server2:
                self.socket_servers.append((socket_server2, socket_port2))
        self.last_read_time = 0
        self.conn = False
        self.conns = []
        if self.type == "socket":
            self.conns = [None] * len(self.socket_servers)

    def connect(self):
        self.last_read_time = 0
        if self.type == "serial":
            if not self.conn:
                self.conn = self.connect_serial(self.serial_port)
        elif self.type == "socket":
            if len(self.conns) != len(self.socket_servers):
                self.conns = [None] * len(self.socket_servers)
            for i, (server, port) in enumerate(self.socket_servers):
                if self.conns[i] is None:
                    sock = self.connect_socket(server, port)
                    if sock:
                        self.conns[i] = sock
            if any(self.conns):
                self.conn = True
        return self.conn

    def connect_serial(self, SERIAL_PORT):
        if SERIAL_PORT is None:
            if platform.system() == "Linux":
                SERIAL_PORT = "/dev/ttyUSB0"
            else:
                SERIAL_PORT = "com3"
        try:
            if serial is None:
                raise Exception("pyserial module not installed")
            ser = serial.Serial(SERIAL_PORT, 9600, timeout=1)
            ser.bytesize = 8
            ser.stopbits = 1
            if not ser.is_open:
                raise Exception("Not ready")
            logging.info(f"[RS485] Serial connected : {ser}")
            return ser
        except Exception as e:
            logging.error(f"[RS485] Serial open failure : {e}")
            return False

    def connect_socket(self, SOCKET_SERVER, SOCKET_PORT):
        sock = socket.socket()
        sock.settimeout(10)
        try:
            sock.connect((SOCKET_SERVER, SOCKET_PORT))
        except Exception as e:
            logging.error(
                f"[RS485] Socket connection failure : {e} | server {SOCKET_SERVER}, port {SOCKET_PORT}"
            )
            return False
        logging.info(
            f"[RS485] Socket connected | server {SOCKET_SERVER}, port {SOCKET_PORT}"
        )
        sock.settimeout(polling_interval + 15)
        return sock

    def read(self):
        if not self.conn:
            return b""
        ret = b""
        if self.type == "serial":
            try:
                if self.conn.in_waiting > 0:
                    ret = self.conn.read(self.conn.in_waiting)
                else:
                    time.sleep(0.01)
                    return b""
            except Exception as e:
                logging.error(f"[RS485] Serial read exception: {e}")
                self.conn = False
                raise
        elif self.type == "socket":
            valid_conns = [s for s in self.conns if s]
            if not valid_conns:
                raise Exception("No socket connected")
            try:
                readable, _, _ = select.select(valid_conns, [], [], 1)
                if readable:
                    sock = readable[0]
                    try:
                        ret = sock.recv(1024)
                    except Exception as e:
                        try:
                            idx = self.conns.index(sock)
                            self.conns[idx].close()
                            self.conns[idx] = None
                        except:
                            pass
                        raise Exception(f"Socket recv error: {e}")

                    if not ret:
                        try:
                            idx = self.conns.index(sock)
                            self.conns[idx].close()
                            self.conns[idx] = None
                        except:
                            pass
                        raise Exception("Socket disconnected")
            except Exception as e:
                if "Socket" in str(e):
                    raise e
                raise Exception(f"read timeout or error: {e}")

        if len(ret) > 0:
            self.last_read_time = time.time()
        return ret

    def write(self, data, socket_index=None):
        if not self.conn:
            return False

        # 쓰기 딜레이
        elapsed = time.time() - self.last_read_time
        if elapsed < read_write_gap:
            time.sleep(read_write_gap - elapsed)

        if self.type == "serial":
            try:
                return self.conn.write(data)
            except:
                return False
        elif self.type == "socket":
            if socket_index is not None:
                if 0 <= socket_index < len(self.conns) and self.conns[socket_index]:
                    try:
                        self.conns[socket_index].send(data)
                        return True
                    except:
                        self.conns[socket_index].close()
                        self.conns[socket_index] = None
                        return False
                return False

            success = False
            for i, sock in enumerate(self.conns):
                if sock:
                    try:
                        sock.send(data)
                        success = True
                    except:
                        sock.close()
                        self.conns[i] = None
            return success
        else:
            return False

    def close(self, socket_index=None):
        if self.conn:
            try:
                if self.type == "serial":
                    self.conn.close()
                    self.conn = False
                elif self.type == "socket":
                    if socket_index is not None:
                        if (
                            0 <= socket_index < len(self.conns)
                            and self.conns[socket_index]
                        ):
                            self.conns[socket_index].close()
                            self.conns[socket_index] = None
                        if not any(self.conns):
                            self.conn = False
                    else:
                        for sock in self.conns:
                            if sock:
                                sock.close()
                        self.conns = [None] * len(self.socket_servers)
                        self.conn = False
            except:
                pass

    def reconnect(self):
        self.close()
        while True:
            logging.info("[RS485] reconnecting to RS485...")
            if self.connect():
                break
            time.sleep(10)


def send(dest, src, cmd, value, log=None, check_ack=True):
    send_lock.acquire()
    ack_data.clear()
    ret = False
    for seq_h in seq_t_dic.keys():
        payload = type_h_dic["send"] + seq_h + "00" + dest + src + cmd + value
        send_data = header_h + payload + chksum(payload) + trailer_h
        try:
            socket_idx = device_socket_map.get(dest)
            if rs485.write(bytearray.fromhex(send_data), socket_idx) == False:
                raise Exception("Not ready")
        except Exception as ex:
            logging.error("[RS485] Write error.[{}]".format(ex))
            break
        if log != None:
            logging.info("[SEND|{}] {}".format(log, send_data))
        if check_ack == False:
            time.sleep(1)
            ret = send_data
            break

        ack_data.append(type_h_dic["ack"] + seq_h + "00" + src + dest + cmd + value)
        if src == device_h_dic["wallpad"] + "00":
            ack_data.append(type_h_dic["ack"] + seq_h + "00" + src + src + cmd + value)
        try:
            ack_q.get(True, 1.3 + 0.2 * random.random())
            if config.get("Log", "show_recv_hex") == "True":
                logging.info("[ACK] OK")
            ret = send_data
            break
        except queue.Empty:
            pass

    if ret == False:
        if rs485.type == "socket":
            logging.info(
                "[RS485] send failed. Attempting to reconnect disconnected sockets."
            )
            rs485.connect()
        else:
            logging.info(
                "[RS485] send failed. closing RS485. it will try to reconnect."
            )
            rs485.close()
    ack_data.clear()
    send_lock.release()
    return ret


def chksum(data_h):
    try:
        sum_buf = sum(bytearray.fromhex(data_h))
        return "{0:02x}".format((sum_buf) % 256)
    except ValueError:
        return "00"


# hex parsing --------------------------------


def parse(hex_data):
    if len(hex_data) < 42:  # 기본 길이 체크
        return None

    header_h = hex_data[:4]
    type_h = hex_data[4:7]
    seq_h = hex_data[7:8]
    monitor_h = hex_data[8:10]
    dest_h = hex_data[10:14]
    src_h = hex_data[14:18]
    cmd_h = hex_data[18:20]
    value_h = hex_data[20:36]
    chksum_h = hex_data[36:38]
    trailer_h = hex_data[38:42]

    data_h = hex_data[4:36]
    payload_h = hex_data[18:36]
    cmd = cmd_t_dic.get(cmd_h)

    ret = {
        "header_h": header_h,
        "type_h": type_h,
        "seq_h": seq_h,
        "monitor_h": monitor_h,
        "dest_h": dest_h,
        "src_h": src_h,
        "cmd_h": cmd_h,
        "value_h": value_h,
        "chksum_h": chksum_h,
        "trailer_h": trailer_h,
        "data_h": data_h,
        "payload_h": payload_h,
        "type": type_t_dic.get(type_h),
        "seq": seq_t_dic.get(seq_h),
        "dest": device_t_dic.get(dest_h[:2]),
        "dest_subid": str(int(dest_h[2:4], 16)),
        "dest_room": room_t_dic.get(dest_h[2:4]),
        "src": device_t_dic.get(src_h[:2]),
        "src_subid": str(int(src_h[2:4], 16)),
        "src_room": room_t_dic.get(src_h[2:4]),
        "cmd": cmd if cmd != None else cmd_h,
        "value": value_h,
        "time": time.time(),
        "flag": None,
    }
    return ret


def thermo_parse(value):
    ret = {
        "heat_mode": (
            "heat"
            if value[:4] == "1100"
            else "fan_only" if value[:4] == "1101" else "off"
        ),
        "set_temp": (
            int(value[4:6], 16)
            if value[:2] == "11"
            else int(config.get("User", "init_temp"))
        ),
        "cur_temp": int(value[8:10], 16),
    }
    return ret


def light_parse(value):
    ret = {}
    user_light_count = int(config.get("User", "light_count"))
    for i in range(1, user_light_count + 1):
        if len(value) < i * 2:
            break
        ret["light_" + str(i)] = "off" if value[i * 2 - 2 : i * 2] == "00" else "on"
    return ret


def plug_parse(value):
    p1 = "on" if value[0:2].lower() == "ff" else "off"
    p2 = "on" if value[2:4].lower() == "ff" else "off"
    return {"plug_1": p1, "plug_2": p2}


def fan_parse(value):
    preset_dic = {"40": "Low", "80": "Medium", "c0": "High"}
    state = "on" if value[:2] == "11" else "off"
    preset = "Off" if state == "off" else preset_dic.get(value[4:6])
    return {"state": state, "preset": preset}


# query device --------------------------


def query(device_h, publish=False, enforce=False):
    for c in list(cache_data):
        if enforce:
            break
        if time.time() - c["time"] > polling_interval:
            break
        if (
            c["type"] == "ack"
            and c["src"] == "wallpad"
            and c["dest_h"] == device_h
            and c["cmd"] != "query"
        ):
            if config.get("Log", "show_query_hex") == "True":
                logging.info(
                    "[cache|{}{}] query cache {}".format(
                        c["dest"], c["dest_subid"], c["data_h"]
                    )
                )
            return c

    if config.get("Log", "show_query_hex") == "True":
        log = "query " + device_t_dic.get(device_h[:2]) + str(int(device_h[2:4], 16))
    else:
        log = None
    return send_wait_response(
        dest=device_h, cmd=cmd_h_dic["query"], log=log, publish=publish
    )


def send_wait_response(
    dest,
    src=device_h_dic["wallpad"] + "00",
    cmd=cmd_h_dic["state"],
    value="0" * 16,
    log=None,
    check_ack=True,
    publish=True,
):
    wait_target.put(dest)
    ret = {"value": "0" * 16, "flag": False}

    if send(dest, src, cmd, value, log, check_ack) != False:
        try:
            ret = wait_q.get(True, 2)
            if publish == True:
                publish_status(ret)
        except queue.Empty:
            pass
    wait_target.get()
    return ret


# ===== elevator call via TCP/IP =====


def call_elevator_tcpip():
    sock = socket.socket()
    sock.settimeout(10)

    APT_SERVER = config.get("Elevator", "tcpip_apt_server")
    APT_PORT = int(config.get("Elevator", "tcpip_apt_port"))

    try:
        sock.connect((APT_SERVER, APT_PORT))
    except Exception as e:
        logging.error(f"Apartment server socket connection failure: {e}")
        return False
    logging.info(f"Apartment server socket connected | server {APT_SERVER}")

    try:
        sock.send(bytearray.fromhex(config.get("Elevator", "tcpip_packet1")))
        rcv = sock.recv(512)
        time.sleep(0.1)
        sock.send(bytearray.fromhex(config.get("Elevator", "tcpip_packet2")))
        rcv = sock.recv(512)
        sock.send(bytearray.fromhex(config.get("Elevator", "tcpip_packet3")))

        for itr in range(100):
            rcv = sock.recv(512)
            if len(rcv) == 0:
                sock.close()
                return True
            rcv_hex = "".join("%02x" % i for i in rcv)
            if rcv_hex == config.get("Elevator", "tcpip_packet4"):
                logging.info("elevator arrived. sending last heartbeat")
                break
        sock.send(bytearray.fromhex(config.get("Elevator", "tcpip_packet2")))
        rcv = sock.recv(512)
        sock.close()
    except Exception as e:
        logging.error(f"Apartment server socket communication failure: {e}")
        return False

    return True


# ===== parse MQTT --> send hex packet =====


def mqtt_on_message(mqttc, obj, msg):
    command = msg.payload.decode("ascii")
    topic_d = msg.topic.split("/")

    if topic_d[-1] != "command":
        return

    logging.info("[MQTT RECV] " + msg.topic + " " + str(msg.payload))

    # thermo heat/off : kocom/room/thermo/3/heat_mode/command
    if "thermo" in topic_d and "heat_mode" in topic_d:
        if len(topic_d) < 4:
            return
        heatmode_dic = {"heat": "1100", "fan_only": "1101", "off": "0100"}
        dev_id = device_h_dic["thermo"] + "{0:02x}".format(int(topic_d[3]))
        q = query(dev_id)
        settemp_hex = (
            "{0:02x}".format(int(config.get("User", "init_temp")))
            if q["flag"] != False
            else "14"
        )
        value = heatmode_dic.get(command) + settemp_hex + "0000000000"
        send_wait_response(dest=dev_id, value=value, log="thermo heatmode")

    # thermo set temp : kocom/room/thermo/3/set_temp/command
    elif "thermo" in topic_d and "set_temp" in topic_d:
        if len(topic_d) < 4:
            return
        dev_id = device_h_dic["thermo"] + "{0:02x}".format(int(topic_d[3]))
        settemp_hex = "{0:02x}".format(int(float(command)))
        value = "1100" + settemp_hex + "0000000000"
        send_wait_response(dest=dev_id, value=value, log="thermo settemp")

    # light on/off : kocom/livingroom/light/1/command
    elif "light" in topic_d:
        if len(topic_d) < 4:
            return
        dev_id = device_h_dic["light"] + room_h_dic.get(topic_d[1], "00")
        value = query(dev_id)["value"]
        onoff_hex = "ff" if command == "on" else "00"
        try:
            light_id = int(topic_d[3])
        except:
            return

        if light_id > 0:
            while light_id > 0:
                n = light_id % 10
                if len(value) >= n * 2:  # 길이 체크
                    value = value[: n * 2 - 2] + onoff_hex + value[n * 2 :]
                send_wait_response(dest=dev_id, value=value, log="light")
                light_id = int(light_id / 10)
        else:
            send_wait_response(dest=dev_id, value=value, log="light")

    # plug on/off : kocom/livingroom/plug/1/command
    elif "plug" in topic_d:
        if len(topic_d) < 5:
            return
        target_room = topic_d[1]
        dev_id = device_h_dic["plug"] + room_h_dic.get(target_room, "00")

        q = query(dev_id)

        if q["flag"] == False:
            logging.warning(
                f"[Plug] Query failed for {target_room}. Command ignored to prevent state mismatch."
            )
            return

        value = q["value"]
        onoff_hex = "ff" if command == "on" else "00"
        num = topic_d[3]

        if num == "1":
            value = onoff_hex + value[2:]
        elif num == "2":
            value = value[:2] + onoff_hex + value[4:]

        send_wait_response(dest=dev_id, value=value, log="plug control")

    # gas off : kocom/livingroom/gas/command
    elif "gas" in topic_d:
        dev_id = device_h_dic["gas"] + room_h_dic.get(topic_d[1], "00")
        if command == "off":
            send_wait_response(dest=dev_id, cmd=cmd_h_dic.get(command), log="gas")
        else:
            logging.info("You can only turn off gas.")

    # elevator on/off : kocom/myhome/elevator/command
    elif "elevator" in topic_d:
        global elevator_timer
        dev_id = device_h_dic["elevator"] + room_h_dic.get(topic_d[1], "00")
        state_on = json.dumps({"state": "on"})
        state_off = json.dumps({"state": "off"})

        if command == "on":
            ret_elevator = None
            if config.get("Elevator", "type", fallback="rs485") == "rs485":
                ret_elevator = send(
                    dest=device_h_dic["wallpad"] + "00",
                    src=dev_id,
                    cmd=cmd_h_dic["on"],
                    value="0" * 16,
                    log="elevator",
                    check_ack=False,
                )
            elif config.get("Elevator", "type", fallback="rs485") == "tcpip":

                threading.Thread(target=call_elevator_tcpip).start()
                ret_elevator = True

            if ret_elevator == False:
                logging.debug("elevator send failed")
                return

            threading.Thread(
                target=mqttc.publish,
                args=("kocom/myhome/elevator/state", state_on, 0, True),
            ).start()

            try:
                if elevator_timer is not None:
                    elevator_timer.cancel()
            except:
                pass

            timeout = int(config.get("Elevator", "call_timeout", fallback=180))
            if config.get("Elevator", "rs485_floor", fallback=None) == None:
                timeout = 5

            elevator_timer = threading.Timer(
                timeout,
                mqttc.publish,
                args=("kocom/myhome/elevator/state", state_off, 0, True),
            )
            elevator_timer.start()

        elif command == "off":
            threading.Thread(
                target=mqttc.publish,
                args=("kocom/myhome/elevator/state", state_off, 0, True),
            ).start()

    # kocom/livingroom/fan/set_preset_mode/command
    elif "fan" in topic_d and "set_preset_mode" in topic_d:
        dev_id = device_h_dic["fan"] + room_h_dic.get(topic_d[1], "00")
        onoff_dic = {"off": "0000", "on": "1101"}
        speed_dic = {"Off": "00", "Low": "40", "Medium": "80", "High": "c0"}
        if command == "Off":
            onoff = onoff_dic["off"]
        elif command in speed_dic.keys():
            onoff = onoff_dic["on"]

        speed = speed_dic.get(command, "00")
        value = onoff + speed + "0" * 10
        send_wait_response(dest=dev_id, value=value, log="fan")

    # kocom/livingroom/fan/command
    elif "fan" in topic_d:
        dev_id = device_h_dic["fan"] + room_h_dic.get(topic_d[1], "00")
        onoff_dic = {"off": "0000", "on": "1101"}
        speed_dic = {"Low": "40", "Medium": "80", "High": "c0"}
        if command == "off":
            value = onoff_dic["off"] + "00" + "0" * 10
        else:
            init_fan_mode = config.get("User", "init_fan_mode")
            value = onoff_dic["on"] + speed_dic.get(init_fan_mode, "40") + "0" * 10
        send_wait_response(dest=dev_id, value=value, log="fan")

    elif "query" in topic_d:
        if command == "PRESS":
            poll_state(enforce=True)


# ===== parse hex packet --> publish MQTT =====


def publish_status(p):
    threading.Thread(target=packet_processor, args=(p,)).start()


def packet_processor(p):
    logtxt = ""
    if p["type"] == "send" and p["dest"] == "wallpad":
        if p["src"] == "thermo" and p["cmd"] == "state":
            state = thermo_parse(p["value"])
            logtxt = "[MQTT publish|thermo] id[{}] data[{}]".format(
                p["src_subid"], state
            )
            mqttc.publish(
                "kocom/room/thermo/" + p["src_subid"] + "/state",
                json.dumps(state),
                retain=True,
            )
        elif p["src"] == "light" and p["cmd"] == "state":
            state = light_parse(p["value"])
            logtxt = "[MQTT publish|light] room[{}] data[{}]".format(
                p["src_room"], state
            )
            mqttc.publish(
                "kocom/{}/light/state".format(p["src_room"]),
                json.dumps(state),
                retain=True,
            )
        elif p["src"] == "plug" and p["cmd"] == "state":
            state = plug_parse(p["value"])
            logtxt = "[MQTT publish|plug] room[{}] data[{}]".format(
                p["src_room"], state
            )
            mqttc.publish(
                "kocom/{}/plug/state".format(p["src_room"]),
                json.dumps(state),
                retain=True,
            )
        elif p["src"] == "fan" and p["cmd"] == "state":
            state = fan_parse(p["value"])
            logtxt = "[MQTT publish|fan] data[{}]".format(state)
            mqttc.publish("kocom/livingroom/fan/state", json.dumps(state), retain=True)
        elif p["src"] == "gas":
            state = {"state": p["cmd"]}
            logtxt = "[MQTT publish|gas] data[{}]".format(state)
            mqttc.publish("kocom/livingroom/gas/state", json.dumps(state), retain=True)

    elif p["type"] == "send" and p["dest"] == "elevator":
        global elevator_timer
        rs485_floor = int(config.get("Elevator", "rs485_floor", fallback=0))
        state = None
        if rs485_floor != 0:
            if p["value"] == "0300000000000000":
                state = {"state": "off"}
        else:
            state = {"state": "off"}

        if state:
            try:
                if elevator_timer is not None:
                    elevator_timer.cancel()
            except:
                pass
            logtxt = "[MQTT publish|elevator] data[{}]".format(state)
            mqttc.publish("kocom/myhome/elevator/state", json.dumps(state), retain=True)

    if logtxt != "" and config.get("Log", "show_mqtt_publish") == "True":
        logging.info(logtxt)


# ===== publish MQTT Devices Discovery =====


def discovery():
    dev_list = [
        x.strip()
        for x in config.get("Device", "enabled", fallback="").split(",")
        if x.strip()
    ]
    dev_list += [
        x.strip()
        for x in config.get("Device", "enabled_2", fallback="").split(",")
        if x.strip()
    ]

    for t in dev_list:
        dev = t.split("_")
        sub = ""
        if len(dev) > 1:
            sub = dev[1]
        publish_discovery(dev[0], sub)
    publish_discovery("query")

    threading.Thread(target=delayed_elevator_state_update).start()


def delayed_elevator_state_update():
    time.sleep(2)
    mqttc.publish(
        "kocom/myhome/elevator/state", json.dumps({"state": "off"}), retain=True
    )


def publish_discovery(dev, sub=""):
    if dev == "fan":
        topic = "homeassistant/fan/kocom_wallpad_fan/config"
        payload = {
            "name": "Kocom Wallpad Fan",
            "cmd_t": "kocom/livingroom/fan/command",
            "stat_t": "kocom/livingroom/fan/state",
            "stat_val_tpl": "{{ value_json.state }}",
            "pr_mode_stat_t": "kocom/livingroom/fan/state",
            "pr_mode_val_tpl": "{{ value_json.preset }}",
            "pr_mode_cmd_t": "kocom/livingroom/fan/set_preset_mode/command",
            "pr_mode_cmd_tpl": "{{ value }}",
            "pr_modes": ["Off", "Low", "Medium", "High"],
            "pl_on": "on",
            "pl_off": "off",
            "qos": 0,
            "uniq_id": "{}_{}_{}".format("kocom", "wallpad", dev),
            "device": {
                "name": "코콤 스마트 월패드",
                "ids": "kocom_smart_wallpad",
                "mf": "KOCOM",
                "mdl": "스마트 월패드",
                "sw": SW_VERSION,
            },
        }
        mqttc.publish(topic, json.dumps(payload), retain=True)

    elif dev == "gas":
        topic = "homeassistant/switch/kocom_wallpad_gas/config"
        payload = {
            "name": "Kocom Wallpad Gas",
            "cmd_t": "kocom/livingroom/gas/command",
            "stat_t": "kocom/livingroom/gas/state",
            "val_tpl": "{{ value_json.state }}",
            "pl_on": "on",
            "pl_off": "off",
            "ic": "mdi:meter-gas",
            "qos": 0,
            "uniq_id": "{}_{}_{}".format("kocom", "wallpad", dev),
            "device": {
                "name": "코콤 스마트 월패드",
                "ids": "kocom_smart_wallpad",
                "mf": "KOCOM",
                "mdl": "스마트 월패드",
                "sw": SW_VERSION,
            },
        }
        mqttc.publish(topic, json.dumps(payload), retain=True)

    elif dev == "elevator":
        topic = "homeassistant/switch/kocom_wallpad_elevator/config"
        payload = {
            "name": "Kocom Wallpad Elevator",
            "cmd_t": "kocom/myhome/elevator/command",
            "stat_t": "kocom/myhome/elevator/state",
            "val_tpl": "{{ value_json.state }}",
            "pl_on": "on",
            "pl_off": "off",
            "ic": "mdi:elevator-passenger",
            "qos": 0,
            "uniq_id": "{}_{}_{}".format("kocom", "wallpad", dev),
            "device": {
                "name": "코콤 스마트 월패드",
                "ids": "kocom_smart_wallpad",
                "mf": "KOCOM",
                "mdl": "스마트 월패드",
                "sw": SW_VERSION,
            },
        }
        mqttc.publish(topic, json.dumps(payload), retain=True)

    elif dev == "light":
        for num in range(1, int(config.get("User", "light_count")) + 1):
            topic = "homeassistant/light/kocom_livingroom_light{}/config".format(num)
            payload = {
                "name": "Kocom livingroom Light{}".format(num),
                "cmd_t": "kocom/livingroom/light/{}/command".format(num),
                "stat_t": "kocom/livingroom/light/state",
                "stat_val_tpl": "{{ value_json.light_" + str(num) + " }}",
                "pl_on": "on",
                "pl_off": "off",
                "qos": 0,
                "uniq_id": "{}_{}_{}{}".format("kocom", "wallpad", dev, num),
                "device": {
                    "name": "코콤 스마트 월패드",
                    "ids": "kocom_smart_wallpad",
                    "mf": "KOCOM",
                    "mdl": "스마트 월패드",
                    "sw": SW_VERSION,
                },
            }
            mqttc.publish(topic, json.dumps(payload), retain=True)

    elif dev == "plug":
        for num in range(1, 3):
            topic = "homeassistant/switch/kocom_livingroom_plug{}/config".format(num)
            payload = {
                "name": "Kocom livingroom Plug{}".format(num),
                "cmd_t": "kocom/livingroom/plug/{}/command".format(num),
                "stat_t": "kocom/livingroom/plug/state",
                "val_tpl": "{{ value_json.plug_" + str(num) + " }}",
                "pl_on": "on",
                "pl_off": "off",
                "qos": 0,
                "uniq_id": "{}_{}_{}{}".format("kocom", "wallpad", dev, num),
                "device": {
                    "name": "코콤 스마트 월패드",
                    "ids": "kocom_smart_wallpad",
                    "mf": "KOCOM",
                    "mdl": "스마트 월패드",
                    "sw": SW_VERSION,
                },
            }
            mqttc.publish(topic, json.dumps(payload), retain=True)

    elif dev == "thermo":
        num = int(room_h_dic.get(sub, "01"))
        topic = "homeassistant/climate/kocom_{}_thermostat/config".format(sub)
        payload = {
            "name": "Kocom {} Thermostat".format(sub),
            "mode_cmd_t": "kocom/room/thermo/{}/heat_mode/command".format(num),
            "mode_stat_t": "kocom/room/thermo/{}/state".format(num),
            "mode_stat_tpl": "{{ value_json.heat_mode }}",
            "temp_cmd_t": "kocom/room/thermo/{}/set_temp/command".format(num),
            "temp_stat_t": "kocom/room/thermo/{}/state".format(num),
            "temp_stat_tpl": "{{ value_json.set_temp }}",
            "curr_temp_t": "kocom/room/thermo/{}/state".format(num),
            "curr_temp_tpl": "{{ value_json.cur_temp }}",
            "modes": ["off", "fan_only", "heat"],
            "min_temp": 20,
            "max_temp": 30,
            "ret": "false",
            "qos": 0,
            "uniq_id": "{}_{}_{}{}".format("kocom", "wallpad", dev, num),
            "device": {
                "name": "코콤 스마트 월패드",
                "ids": "kocom_smart_wallpad",
                "mf": "KOCOM",
                "mdl": "스마트 월패드",
                "sw": SW_VERSION,
            },
        }
        mqttc.publish(topic, json.dumps(payload), retain=True)

    elif dev == "query":
        topic = "homeassistant/button/kocom_wallpad_query/config"
        payload = {
            "name": "Kocom Wallpad Query",
            "cmd_t": "kocom/myhome/query/command",
            "qos": 0,
            "uniq_id": "{}_{}_{}".format("kocom", "wallpad", dev),
            "device": {
                "name": "코콤 스마트 월패드",
                "ids": "kocom_smart_wallpad",
                "mf": "KOCOM",
                "mdl": "스마트 월패드",
                "sw": SW_VERSION,
            },
        }
        mqttc.publish(topic, json.dumps(payload), retain=True)


# ===== thread functions =====


def poll_state(enforce=False):
    global poll_timer
    try:
        poll_timer.cancel()
    except:
        pass

    if rs485.type == "socket":
        rs485.connect()

    dev_list = [
        x.strip()
        for x in config.get("Device", "enabled", fallback="").split(",")
        if x.strip()
    ]
    dev_list += [
        x.strip()
        for x in config.get("Device", "enabled_2", fallback="").split(",")
        if x.strip()
    ]
    no_polling_list = ["wallpad", "elevator"]

    # Thread health check
    for thread_instance in thread_list:
        if not thread_instance.is_alive():
            logging.error(
                "[THREAD] {} is not active. starting.".format(thread_instance.name)
            )
            try:
                thread_instance.start()
            except RuntimeError:
                pass  # already started

    for t in dev_list:
        dev = t.split("_")
        if dev[0] in no_polling_list:
            continue

        dev_id = device_h_dic.get(dev[0])
        sub_id = room_h_dic.get(dev[1]) if len(dev) > 1 else "00"

        if dev_id != None and sub_id != None:
            if query(dev_id + sub_id, publish=True, enforce=enforce)["flag"] == False:
                break
            time.sleep(1)

    poll_timer = threading.Timer(polling_interval, poll_state)
    poll_timer.start()


def read_serial():
    global poll_timer
    buf = ""
    not_parsed_buf = ""
    while True:
        try:
            d_list = rs485.read()
            if not d_list:
                continue

            for d in d_list:
                if isinstance(d, int):
                    hex_d = "{0:02x}".format(d)
                else:
                    hex_d = "{0:02x}".format(ord(d))

                buf += hex_d

                if buf[: len(header_h)] != header_h[: len(buf)]:
                    not_parsed_buf += buf
                    buf = ""
                    frame_start = not_parsed_buf.find(header_h)
                    if frame_start >= 0:
                        buf = not_parsed_buf[frame_start:]
                        not_parsed_buf = not_parsed_buf[:frame_start]
                    continue

                if len(buf) == (packet_size * 2):
                    chksum_calc = chksum(buf[len(header_h) : chksum_position * 2])
                    chksum_buf = buf[chksum_position * 2 : chksum_position * 2 + 2]

                    if (
                        chksum_calc == chksum_buf
                        and buf[-len(trailer_h) :] == trailer_h
                    ):
                        msg_q.put(buf)
                        buf = ""
                    else:
                        frame_start = buf.find(header_h, 2)
                        if frame_start >= 0:
                            buf = buf[frame_start:]
                        else:
                            buf = ""
        except Exception as ex:
            logging.error("*** Read error.[{}]".format(ex))
            try:
                poll_timer.cancel()
            except:
                pass
            cache_data.clear()
            if rs485.type == "socket":
                rs485.connect()
            else:
                rs485.reconnect()
            poll_timer = threading.Timer(2, poll_state)
            poll_timer.start()


def listen_hexdata():
    while True:
        d = msg_q.get()

        if config.get("Log", "show_recv_hex") == "True":
            logging.info("[recv] " + d)

        p_ret = parse(d)
        if not p_ret:
            continue

        cache_data.insert(0, p_ret)
        if len(cache_data) > BUF_SIZE:
            del cache_data[-1]

        if p_ret["data_h"] in ack_data:
            ack_q.put(d)
            continue

        if not wait_target.empty():
            try:
                target_dest = wait_target.queue[0]
            except IndexError:
                target_dest = None

            if (
                target_dest
                and p_ret["dest_h"] == target_dest
                and p_ret["type"] == "ack"
            ):
                if len(ack_data) != 0:
                    ack_q.put(d)
                    time.sleep(0.5)
                wait_q.put(p_ret)
                continue
        publish_status(p_ret)


# ========== Main ==========

if __name__ == "__main__":
    logging.basicConfig(
        format="%(levelname)s[%(asctime)s]:%(message)s ", level=logging.DEBUG
    )

    if not os.path.exists(CONFIG_FILE):
        logging.error(f"[CONFIG] {CONFIG_FILE} not found.")
        sys.exit(1)

    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)

    # Device Mapping
    for t in [
        x.strip()
        for x in config.get("Device", "enabled", fallback="").split(",")
        if x.strip()
    ]:
        dev = t.split("_")
        dev_id = device_h_dic.get(dev[0])
        sub_id = room_h_dic.get(dev[1]) if len(dev) > 1 else "00"
        if dev_id:
            device_socket_map[dev_id + sub_id] = 0

    for t in [
        x.strip()
        for x in config.get("Device", "enabled_2", fallback="").split(",")
        if x.strip()
    ]:
        dev = t.split("_")
        dev_id = device_h_dic.get(dev[0])
        sub_id = room_h_dic.get(dev[1]) if len(dev) > 1 else "00"
        if dev_id:
            device_socket_map[dev_id + sub_id] = 1

    # RS485 Init
    rs485_type = config.get("RS485", "type", fallback="serial")
    if rs485_type == "serial":
        rs485 = RS485Wrapper(
            serial_port=config.get("RS485", "serial_port", fallback=None)
        )
    elif rs485_type == "socket":
        rs485 = RS485Wrapper(
            socket_server=config.get("RS485", "socket_server"),
            socket_port=int(config.get("RS485", "socket_port")),
            socket_server2=config.get("RS485", "socket_server2", fallback=None),
            socket_port2=int(config.get("RS485", "socket_port2", fallback=0)),
        )
    else:
        logging.error("[CONFIG] RS485 type error (serial or socket)")
        sys.exit(1)

    if rs485.connect() == False:
        logging.error("[RS485] connection error. exit")
        sys.exit(1)

    mqttc = init_mqttc()
    if mqttc == False:
        logging.error("[MQTT] conection error. exit")
        sys.exit(1)

    msg_q = queue.Queue(BUF_SIZE)
    ack_q = queue.Queue(1)
    ack_data = []
    wait_q = queue.Queue(1)
    wait_target = queue.Queue(1)
    send_lock = threading.Lock()
    poll_timer = threading.Timer(1, poll_state)

    cache_data = []

    thread_list = []
    thread_list.append(threading.Thread(target=read_serial, name="read_serial"))
    thread_list.append(threading.Thread(target=listen_hexdata, name="listen_hexdata"))
    for thread_instance in thread_list:
        thread_instance.start()

    poll_timer.start()
    discovery()
