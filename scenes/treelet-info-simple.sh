# rm -rf ~/pbrt/scenes/X/*
# ~/pbrt/build/pbrt --dumpscene X/ --nomaterial killeroo-dump.pbrt 
~/pbrt/build/pbrt-treelet-info info ./X 10 2>&1 | tee dump.txt