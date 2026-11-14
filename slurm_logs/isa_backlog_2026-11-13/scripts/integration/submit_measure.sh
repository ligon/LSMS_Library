#!/bin/bash
# Submit the task-i cold builds: integrated B and each owning stream head.
cd /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/integration
B=fbed85bba327c32725a14e201f0e0079ef73aaa9
CROPKG=bda250217906c4303e79bfab23a71a5781168708; MALAWI=5093b49398424e953fe049cfe12b3f29eb991443
BURKINA=f962ef7c9b1cb6c4bc699a20aa5845e94abdc4de; COPING=2ee44de32c1d13ad8f5c1b4ea13ac3c5b7e9e847
MANQUANT=63aa569cad2d2d4d2d8f9f928a686dd443af931b; NIGERIA=dc3db3dc795d99fa82fbbcfb697323e000b4be1f
GNB=b7e65b83e40f5a6026493f5f42e48f49b09653f6; BENIN=2aa1e428736450e5190992019c1b5eaa0b71b5ba
SB="sbatch -A co_carleton -p savio4_htc -q carleton_htc4_normal -N 1 -n 1 --exclude=n0035.savio4,n0034.savio4,n0046.savio4 --requeue -o logs/%x-%j.out"
H="-c 4 --mem=72G -t 8:00:00"; L="-c 4 --mem=40G -t 5:00:00"
KG='food_quantities:{"units":"kgs"}'
sub() { local res="$1" name="$2"; shift 2; $SB $res -J "$name" scripts/measure.sbatch "$@" | awk -v n="$name" '{print n, $NF}'; }
sub "$H" m-NGA-B $B Nigeria food_acquired "$KG" food_prices crop_production individual_education community_prices shocks assets
sub "$H" m-NGA-nga $NIGERIA Nigeria food_acquired "$KG" food_prices crop_production individual_education community_prices
sub "$L" m-NGA-cop $COPING Nigeria shocks assets
sub "$L" m-NER-B $B Niger plot_inputs shocks crop_production
sub "$L" m-NER-man $MANQUANT Niger plot_inputs
sub "$L" m-NER-cop $COPING Niger shocks
sub "$L" m-NER-ckg $CROPKG Niger crop_production
sub "$L" m-MLI-B $B Mali shocks crop_production
sub "$L" m-MLI-cop $COPING Mali shocks
sub "$L" m-MLI-ckg $CROPKG Mali crop_production
sub "$L" m-SEN-B $B Senegal plot_inputs shocks crop_production plot_features
sub "$L" m-SEN-man $MANQUANT Senegal plot_inputs crop_production plot_features
sub "$L" m-SEN-cop $COPING Senegal shocks
sub "$L" m-TGO-B $B Togo plot_inputs shocks
sub "$L" m-TGO-man $MANQUANT Togo plot_inputs
sub "$L" m-TGO-cop $COPING Togo shocks
sub "$L" m-CIV-B $B CotedIvoire plot_inputs shocks
sub "$L" m-CIV-man $MANQUANT CotedIvoire plot_inputs
sub "$L" m-CIV-cop $COPING CotedIvoire shocks
sub "$H" m-GNB-B $B Guinea-Bissau sample household_roster household_characteristics food_acquired "$KG" food_prices plot_inputs crop_production assets shocks cluster_features
sub "$H" m-GNB-gnb $GNB Guinea-Bissau sample household_roster household_characteristics food_acquired "$KG" food_prices plot_inputs crop_production assets shocks cluster_features
sub "$L" m-GNB-cop $COPING Guinea-Bissau shocks
sub "$H" m-BFA-B $B Burkina_Faso crop_production interview_date food_acquired plot_inputs shocks
sub "$H" m-BFA-bfa $BURKINA Burkina_Faso crop_production interview_date food_acquired plot_inputs
sub "$L" m-BFA-cop $COPING Burkina_Faso shocks
sub "$L" m-UGA-B $B Uganda shocks assets
sub "$L" m-UGA-cop $COPING Uganda shocks assets
sub "$H" m-BEN-B $B Benin sample household_roster shocks food_acquired plot_inputs crop_production
sub "$H" m-BEN-ben $BENIN Benin sample household_roster shocks food_acquired plot_inputs crop_production
sub "$L" m-BEN-cop $COPING Benin shocks
sub "$H" m-MWI-B $B Malawi crop_production plot_inputs shocks
sub "$H" m-MWI-mwi $MALAWI Malawi crop_production plot_inputs
sub "$L" m-MWI-cop $COPING Malawi shocks
$SB -c 4 --mem=40G -t 4:00:00 -J int-windows scripts/windows.sbatch $B | awk '{print "windows", $NF}'
