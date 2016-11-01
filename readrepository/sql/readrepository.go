// Copyright (c) 2015 - Max Persson <max@looplab.se>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sql

import (
	"reflect"
	"time"

	"github.com/jinzhu/gorm"

	eh "github.com/looplab/eventhorizon"
)

// MongoReadRepository implements an MongoDB repository of read models.
type SqlReadRepository struct {
	db      gorm.DB
	factory func() interface{}
}

type SqlBaseEntry struct {
	ID        eh.UUID    `gorm:"primary_key"`
	CreatedAt time.Time  `sql:"index"`
	UpdatedAt time.Time  `sql:"index"`
	DeletedAt *time.Time `sql:"index"`
}

// NewMongoReadRepository creates a new MongoReadRepository.
func NewSqlReadRepository(db gorm.DB, factory func() interface{}) (*SqlReadRepository, error) {
	r := &SqlReadRepository{
		db:      db,
		factory: factory,
	}

	model := factory()

	db.AutoMigrate(model)

	return r, nil
}

// Save saves a read model with id to the repository.
func (r *SqlReadRepository) Save(id eh.UUID, model interface{}) error {
	m := r.factory()

	var c uint
	r.db.Model(m).Where("id =?", id).Count(&c)
	if c > 0 {
		r.db.Save(model)
	} else {
		r.db.Create(model)
	}
	return nil
}

// Find returns one read model with using an id. Returns
// ErrModelNotFound if no model could be found.
func (r *SqlReadRepository) Find(id eh.UUID) (interface{}, error) {
	model := r.factory()

	r.db.Where("id =?", id).First(model)
	return model, nil
}

// FindCustom uses a callback to specify a custom query.
func (r *SqlReadRepository) FindPage(query, order string, page, size uint) ([]interface{}, uint, error) {

	m := r.factory()

	begin := page * size
	var allcount uint
	if query == "" {
		r.db.Model(m).Count(&allcount)
	} else {
		r.db.Model(m).Where(query).Count(&allcount)
	}

	if begin > allcount {
		return []interface{}{}, allcount, nil
	}

	count := size
	if allcount-begin < size { //last page
		count = allcount - begin
	}

	if order == "" {
		order = "updated_at"
	}

	v := reflect.ValueOf(m).Elem()
	sv := reflect.MakeSlice(reflect.SliceOf(v.Type()), 0, 0)

	snv := reflect.New(sv.Type())
	result := snv.Interface()

	if query != "" {
		r.db.Where(query).Order(order).Offset(begin).Limit(count).Find(result)
	} else {
		r.db.Order(order).Offset(begin).Limit(count).Find(result)
	}

	rv := reflect.Indirect(reflect.ValueOf(result))

	ret := make([]interface{}, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		ret[i] = rv.Index(i).Interface()
	}

	return ret, allcount, nil
}

// FindAll returns all read models in the repository.
func (r *SqlReadRepository) FindAll() ([]interface{}, error) {
	m := r.factory()

	var allcount uint
	r.db.Model(m).Count(&allcount)

	v := reflect.ValueOf(m).Elem()
	sv := reflect.MakeSlice(reflect.SliceOf(v.Type()), 0, 0)

	snv := reflect.New(sv.Type())
	result := snv.Interface()

	r.db.Find(result)

	rv := reflect.Indirect(reflect.ValueOf(result))

	ret := make([]interface{}, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		ret[i] = rv.Index(i).Interface()
	}

	return ret, nil
}

// Remove removes a read model with id from the repository. Returns
// ErrModelNotFound if no model could be found.
func (r *SqlReadRepository) Remove(id eh.UUID) error {
	m := r.factory()

	r.db.Where("id =", id).Delete(m)

	return nil
}
