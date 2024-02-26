yum -y update

yum -y groupinstall "Development Tools"

yum install -y wget

wget http://ftp.gnu.org/gnu/autoconf/autoconf-2.69.tar.gz
tar xvf autoconf-2.69.tar.gz
cd autoconf-2.69
./configure --prefix=/usr
make
make install
cd ..

wget http://ftp.gnu.org/gnu/automake/automake-1.14.tar.gz
tar xvf automake-1.14.tar.gz
cd automake-1.14
./configure --prefix=/usr
make
make install
cd ..

wget http://ftp.gnu.org/gnu/bison/bison-2.5.1.tar.gz
tar xvf bison-2.5.1.tar.gz
cd bison-2.5.1
./configure --prefix=/usr
make
make install
cd ..

yum -y install libevent-devel zlib-devel openssl-devel
wget http://sourceforge.net/projects/boost/files/boost/1.56.0/boost_1_56_0.tar.gz
tar xvf boost_1_56_0.tar.gz
cd boost_1_56_0
./bootstrap.sh
./b2 install
git clone https://github.com/apache/thrift.git
cd thrift
./bootstrap.sh
./configure --with-lua=no
make
make install
