
yum -y install libevent-devel zlib-devel openssl-devel
wget http://sourceforge.net/projects/boost/files/boost/1.56.0/boost_1_56_0.tar.gz
tar xvf boost_1_56_0.tar.gz
cd boost_1_56_0
./bootstrap.sh
./b2 install
git clone https://github.com/apache/thrift.git
cd thrift
./bootstrap.sh
cat configure
./configure --with-lua=no --with-qt5=no
make
make install
