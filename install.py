#!/usr/bin/python
import os
import subprocess
import sys

def InstallGrpc():
    subprocess.call(["sudo", "apt-get", "install", "build-essential", "autoconf", "libtool", "curl", "cmake", "git", "pkg-config"])
    os.system("git clone -b $(curl -L http://grpc.io/release) https://github.com/grpc/grpc")
    subprocess.Popen(["git", "submodule", "update", "--init"], cwd="grpc/")
    subprocess.Popen(["make"], cwd="grpc/")
    subprocess.Popen(["sudo", "make", "install"], cwd="grpc/")

def InstallProtobuf():
    subprocess.call(["wget", "https://github.com/google/protobuf/releases/download/v3.2.0/protobuf-cpp-3.2.0.tar.gz"])
    subprocess.call(["tar", "-xzvf", "protobuf-cpp-3.2.0.tar.gz"]) 
    subprocess.Popen(["./configure"], cwd="protobuf-3.2.0")
    subprocess.Popen(["make"], cwd="protobuf-3.2.0")
    subprocess.Popen(["make", "check"], cwd="protobuf-3.2.0")
    subprocess.Popen(["sudo", "make", "install"], cwd="protobuf-3.2.0")
    subprocess.Popen(["sudo", "ldconfig"], cwd="protobuf-3.2.0")

def InstallOpenSSL():
    subprocess.call(["sudo", "apt-get", "install", "openssl"])
    subprocess.call(["sudo", "apt-get", "install", "libssl-dev"])

def main():
    print "Installing gRPC....\n"
    InstallGrpc()
    print "Installed gRPC....\n"
    print "Installing Protobuf....\n"
    InstallProtobuf()
    print "Installed Protobuf....\n"
    InstallOpenSSL()

main()
