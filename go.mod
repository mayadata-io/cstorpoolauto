module cstorpoolauto

go 1.12

require (
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b
	github.com/pkg/errors v0.8.1
	golang.org/x/crypto v0.0.0-20191002192127-34f69633bfdc // indirect
	golang.org/x/time v0.0.0-20190921001708-c4c64cad1fd0 // indirect
	k8s.io/apimachinery v0.0.0-20191006235458-f9f2f3f8ab02
	k8s.io/utils v0.0.0-20190923111123-69764acb6e8e // indirect
	openebs.io/metac v0.1.1-0.20200116003950-9c9ab7d2e4c2
)

replace openebs.io/metac => github.com/AmitKumarDas/metac v0.1.1-0.20200116003950-9c9ab7d2e4c2
