sudo apt-get update
sudo apt --assume-yes install default-jre maven docker.io python3-pip fish
git clone https://github.com/park-project/park.git ~/park
cd ~/park
git checkout qopt
mv ~/payload/park_agents/ ~/park_agents
cd ~/park_agents
git checkout qopt-agent
pip3 install -r requirements.txt
cd ~/
git clone https://github.com/hongzimao/gcn_pytorch.git ~/gcn_pytorch
echo "export PYTHONPATH=$PYTHONPATH:/home/ubuntu/park/:/home/ubuntu/gcn_pytorch" >> ~/.bashrc
