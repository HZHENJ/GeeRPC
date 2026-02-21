package geerpc

import (
	"go/ast"
	"log"
	"reflect"
	"sync/atomic"
)

type methodType struct {
	method    reflect.Method // 方法名
	ArgType   reflect.Type   // 第一个参数类型
	ReplyType reflect.Type   // 第二个参数类型
	numCalls  uint64         // 统计方法调用次数
}

func (m *methodType) NumCalls() uint64 {
	return atomic.LoadUint64(&m.numCalls)
}

// newArgv
func (m *methodType) newArgv() reflect.Value {
	var argv reflect.Value

	// arg may be a pointer type, or a value type
	if m.ArgType.Kind() == reflect.Ptr {
		argv = reflect.New(m.ArgType.Elem())
	} else {
		argv = reflect.New(m.ArgType).Elem()
	}
	return argv
}

// newReply
func (m *methodType) newReplyv() reflect.Value {
	// reply must be a pointer type
	replyv := reflect.New(m.ReplyType.Elem())
	switch m.ReplyType.Kind() {
	case reflect.Map:
		replyv.Elem().Set(reflect.MakeMap(m.ReplyType.Elem()))
	case reflect.Slice:
		replyv.Elem().Set(reflect.MakeSlice(m.ReplyType.Elem(), 0, 0))
	}
	return replyv
}

type service struct {
	name   string                 // 映射的结构体名称
	typ    reflect.Type           // 结构体类型
	rcvr   reflect.Value          // 结构体实例本身 在调用时需要rcvr作为第0个参数
	method map[string]*methodType // 存储映射的结构体的所有符合条件的方法
}

// newService
func newService(rcvr interface{}) *service {
	s := new(service)
	s.rcvr = reflect.ValueOf(rcvr)
	// reflect.Indirect可以直接拿到基础类型
	// 这里是为了拿到结构体的名字
	s.name = reflect.Indirect(s.rcvr).Type().Name()
	s.typ = reflect.TypeOf(rcvr) // 拿到类型信息

	// 服务名必须是大写字母开头，否则无法调用
	if !ast.IsExported(s.name) {
		log.Fatalf("rpc server: %s is not a valid service name", s.name)
	}

	s.registerMethods()
	return s
}

// registerMethods 获取结构体中的所有方法
func (s *service) registerMethods() {
	s.method = make(map[string]*methodType)
	for i := 0; i < s.typ.NumMethod(); i++ {
		method := s.typ.Method(i)
		mType := method.Type

		// 参数和返回值数量检查
		// 为什么传入的参数是三个？在反射中结构体的方法第一个参数永远是它自己Receiver
		// 例如：func (t *Arith) Add(args, reply) 这里的t就是第0个参数
		if mType.NumIn() != 3 || mType.NumOut() != 1 {
			continue
		}

		// 返回值类型检查
		// 唯一的返回值必须是error类型
		if mType.Out(0) != reflect.TypeOf((*error)(nil)).Elem() {
			continue
		}

		// 获取第1个参数Args 第二个参数Reply
		argType, replyType := mType.In(1), mType.In(2)

		// 参数类型必须是导出的（首字母大写）或者是Go内置基本类型（int string...
		// 如果传递私有结构体，Gob序列化时会报错..
		if !isExportedOrBuiltinType(argType) || !isExportedOrBuiltinType(replyType) {
			continue
		}
		s.method[method.Name] = &methodType{
			method:    method,
			ArgType:   argType,
			ReplyType: replyType,
		}
		log.Printf("rpc server: register %s.%s\n", s.name, method.Name)
	}
}

func (s *service) call(m *methodType, argv, replyv reflect.Value) error {
	// 原子操作，防止冲突
	// 不使用互斥锁的原因？上下文切换的代价很高
	atomic.AddUint64(&m.numCalls, 1)

	f := m.method.Func
	returnValues := f.Call([]reflect.Value{s.rcvr, argv, replyv})
	if errInter := returnValues[0].Interface(); errInter != nil {
		return errInter.(error)
	}
	return nil
}

func isExportedOrBuiltinType(t reflect.Type) bool {
	return ast.IsExported(t.Name()) || t.PkgPath() == ""
}
