#!/usr/bin/env python3
import json, socket, threading, tkinter as tk
from dataclasses import dataclass, asdict
from tkinter import ttk, messagebox, filedialog

@dataclass
class Config:
    max_message_length:int=10_000_000; opcode_pos:int=1; version_pos:int=2
    route_len_pos:int=3; route_order:int=1; payload_order:int=3; options_order:int=2
    options_count_pos:int=4; options_key_first:bool=True; header_length:int=29
    def validate(self):
        if self.header_length < 29: raise ValueError('header_length must be >= 29')
        p=[self.opcode_pos,self.version_pos,self.route_len_pos,self.options_count_pos]
        if len(set(p)) != 4 or any(x < 1 or x > self.header_length-12 for x in p): raise ValueError('invalid header positions')
        if sorted([self.route_order,self.payload_order,self.options_order]) != [1,2,3]: raise ValueError('orders must be 1,2,3')

def options_encode(items, key_first):
    out=bytearray()
    for k,v in items:
        a,b=k.encode(),v.encode()
        if not a or not b: raise ValueError('empty option')
        if len(a)>65535 or len(b)>65535: raise ValueError('option too large')
        out += len(a).to_bytes(2,'big')+len(b).to_bytes(2,'big')+(a+b if key_first else b+a)
    return bytes(out)

def encode(c, opcode, version, req_id, route, payload, options):
    c.validate(); r=route.encode(); o=options_encode(options,c.options_key_first)
    if len(r)>255 or len(options)>255: raise ValueError('route/options count too large')
    if len(r)+len(payload)+len(o)>c.max_message_length: raise ValueError('message too large')
    h=bytearray(c.header_length); h[c.opcode_pos-1]=opcode; h[c.version_pos-1]=version; h[c.route_len_pos-1]=len(r); h[c.options_count_pos-1]=len(options)
    h[-12:-8]=len(o).to_bytes(4,'big'); h[-8:-4]=len(payload).to_bytes(4,'big'); h[-4:]=req_id.to_bytes(4,'big')
    parts={c.route_order:r,c.payload_order:payload,c.options_order:o}
    return bytes(h)+b''.join(parts[i] for i in (1,2,3))

def decode(c, frame):
    c.validate(); h=frame[:c.header_length]
    if len(frame)<c.header_length: raise ValueError('short header')
    rl=h[c.route_len_pos-1]; oc=h[c.options_count_pos-1]; ol=int.from_bytes(h[-12:-8],'big'); pl=int.from_bytes(h[-8:-4],'big'); rid=int.from_bytes(h[-4:],'big'); body=rl+ol+pl
    if len(frame)!=c.header_length+body or body>c.max_message_length: raise ValueError('invalid frame length')
    off=c.header_length; parts={}
    for i,n in ((c.route_order,rl),(c.payload_order,pl),(c.options_order,ol)): parts[i]=frame[off:off+n]; off+=n
    raw=parts[c.options_order]; opts=[]; p=0
    for _ in range(oc):
        if p+4>len(raw): raise ValueError('truncated options')
        kl=int.from_bytes(raw[p:p+2],'big'); vl=int.from_bytes(raw[p+2:p+4],'big'); p+=4
        if p+kl+vl>len(raw): raise ValueError('truncated option')
        a,b=raw[p:p+kl],raw[p+kl:p+kl+vl]; p+=kl+vl; opts.append(((a,b) if c.options_key_first else (b,a))[0].decode(), ((a,b) if c.options_key_first else (b,a))[1].decode())
    if p!=len(raw): raise ValueError('trailing options')
    return {'opcode':h[c.opcode_pos-1],'version':h[c.version_pos-1],'request_id':rid,'route':parts[c.route_order].decode(),'payload':parts[c.payload_order].decode(errors='replace'),'options':opts}

class App:
    def __init__(self,root):
        self.root=root; root.title('MMA protocol client'); self.sock=None; self.v={}; self.build()
    def field(self,parent,row,name,val):
        self.v[name]=tk.BooleanVar(value=val) if isinstance(val,bool) else tk.StringVar(value=str(val)); ttk.Label(parent,text=name).grid(row=row,column=0,sticky='w'); ttk.Entry(parent,textvariable=self.v[name]).grid(row=row,column=1,sticky='ew')
    def build(self):
        n=ttk.Notebook(self.root); n.pack(fill='both',expand=True); a=ttk.Frame(n,padding=8); b=ttk.Frame(n,padding=8); n.add(a,text='Connection / codec'); n.add(b,text='Request')
        for i,(k,v) in enumerate([('host','127.0.0.1'),('port',8080),('header_length',29),('max_message_length',10000000),('opcode_pos',1),('version_pos',2),('route_len_pos',3),('options_count_pos',4),('route_order',1),('payload_order',3),('options_order',2)]): self.field(a,i,k,v)
        self.v['options_key_first']=tk.BooleanVar(value=True); ttk.Checkbutton(a,text='options_key_first',variable=self.v['options_key_first']).grid(row=11,columnspan=2,sticky='w'); ttk.Button(a,text='Connect',command=self.connect).grid(row=12,columnspan=2,sticky='ew'); ttk.Button(a,text='Save config',command=self.save).grid(row=13,columnspan=2,sticky='ew')
        for i,(k,v) in enumerate([('opcode',2),('version',1),('request_id',1),('route','')]): self.field(b,i,k,v)
        ttk.Label(b,text='payload UTF-8').grid(row=4,column=0); self.payload=tk.Text(b,height=5); self.payload.grid(row=4,column=1,sticky='nsew'); ttk.Label(b,text='options key=value').grid(row=5,column=0); self.opts=tk.Text(b,height=5); self.opts.grid(row=5,column=1,sticky='nsew'); ttk.Button(b,text='Send',command=self.send).grid(row=6,columnspan=2,sticky='ew'); self.out=tk.Text(b,state='disabled'); self.out.grid(row=7,columnspan=2,sticky='nsew'); b.columnconfigure(1,weight=1); b.rowconfigure(7,weight=1)
    def cfg(self):
        keys=['header_length','max_message_length','opcode_pos','version_pos','route_len_pos','options_count_pos','route_order','payload_order','options_order']; return Config(**{k:int(self.v[k].get()) for k in keys},options_key_first=bool(self.v['options_key_first'].get()))
    def log(self,x): self.out.config(state='normal'); self.out.insert('end',x+'\n\n'); self.out.config(state='disabled')
    def connect(self):
        try: self.cfg().validate(); self.sock=socket.create_connection((self.v['host'].get(),int(self.v['port'].get()))); threading.Thread(target=self.read,daemon=True).start(); self.log('connected')
        except Exception as e: messagebox.showerror('Connect',str(e))
    def send(self):
        try:
            items=[tuple(x.split('=',1)) for x in self.opts.get('1.0','end').splitlines() if x.strip()]; packet=encode(self.cfg(),int(self.v['opcode'].get()),int(self.v['version'].get()),int(self.v['request_id'].get()),self.v['route'].get(),self.payload.get('1.0','end-1c').encode(),items); self.sock.sendall(packet); self.log('sent %d bytes'%len(packet))
        except Exception as e: messagebox.showerror('Send',str(e))
    def read(self):
        buf=b''
        while self.sock:
            try:
                buf+=self.sock.recv(65536); c=self.cfg(); h=c.header_length
                if len(buf)>=h:
                    size=h+int.from_bytes(buf[h-12:h-8],'big')+int.from_bytes(buf[h-8:h-4],'big')+buf[c.route_len_pos-1]
                    if len(buf)>=size: self.log(json.dumps(decode(c,buf[:size]),ensure_ascii=False,indent=2)); buf=buf[size:]
            except Exception as e: self.log(str(e)); return
    def save(self):
        p=filedialog.asksaveasfilename(defaultextension='.json');
        if p: json.dump({k:v.get() for k,v in self.v.items()},open(p,'w'),indent=2)

root=tk.Tk(); App(root); root.mainloop()
