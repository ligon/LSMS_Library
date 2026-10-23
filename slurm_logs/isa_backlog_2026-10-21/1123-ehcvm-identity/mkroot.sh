# usage: mkroot.sh <checkout> <dest> <inject: none|20mil|1519>
set -e
CK=$1; D=$2; INJ=$3
rm -rf $D; mkdir -p $D/countries $D/data
ln -s /global/scratch/fsa/fc_jevons/ligon/cache/lsms_library/dvc-cache $D/data/dvc-cache
cd $CK/lsms_library/countries
cp -r .dvc .dvcignore dvc.yaml $D/countries/
cp -r Niger $D/countries/
find $D/countries -name __pycache__ -prune -exec rm -rf {} +
O=$D/countries/Niger/_/categorical_mapping.org
case $INJ in
  20mil) sed -i 's/^| 20                            | Autre crop          |$/| 20                            | Mil                 |/' $O ;;
  1519) sed -i -e "s/^| Semences d'autres céréales    | Autre céréale       |$/| Semences d'autres céréales    | Autre crop          |/" \
               -e "s/^| Plants\/boutures de tubercules | Autre tubercule     |$/| Plants\/boutures de tubercules | Autre crop          |/" $O ;;
esac
diff $CK/lsms_library/countries/Niger/_/categorical_mapping.org $O || true
