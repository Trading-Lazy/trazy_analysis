git fetch actionsapi master

branch_or_tag=$1
if [ $# -eq 1 ]
then
    echo ${branch_or_tag}
else
    echo "Invalid argument please pass only one argument that is a branch or a tag name"
fi
git rm -r actionsapi
git commit -m "Delete actionsapi"
rm -rf actionsapi
git subtree add --prefix actionsapi actionsapi ${branch_or_tag} --squash
