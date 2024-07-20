package blnk

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/northstar-pay/nucleus/model"

	"github.com/brianvoe/gofakeit/v6"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func TestCreateIdentity(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	d, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}

	identity := model.Identity{
		IdentityType:     "individual",
		OrganizationName: "",
		Category:         "",
		FirstName:        gofakeit.FirstName(),
		LastName:         gofakeit.LastName(),
		OtherNames:       gofakeit.LastName(),
		Gender:           gofakeit.Gender(),
		DOB:              gofakeit.Date(),
		EmailAddress:     gofakeit.Email(),
		PhoneNumber:      gofakeit.Phone(),
		Nationality:      gofakeit.Country(),
		Street:           gofakeit.Street(),
		Country:          gofakeit.Country(),
		State:            gofakeit.State(),
		PostCode:         "0000",
		City:             gofakeit.City(),
		MetaData:         nil,
	}
	metaDataJSON, _ := json.Marshal(identity.MetaData)

	mock.ExpectExec("INSERT INTO identity").
		WithArgs(sqlmock.AnyArg(), identity.IdentityType, identity.FirstName, identity.LastName, identity.OtherNames, identity.Gender, identity.DOB, identity.EmailAddress, identity.PhoneNumber, identity.Nationality, identity.OrganizationName, identity.Category, identity.Street, identity.Country, identity.State, identity.PostCode, identity.City, sqlmock.AnyArg(), metaDataJSON).
		WillReturnResult(sqlmock.NewResult(1, 1))

	result, err := d.CreateIdentity(identity)
	assert.NoError(t, err)
	assert.NotEmpty(t, result.IdentityID)
	assert.Equal(t, identity.FirstName, result.FirstName)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestGetIdentity(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	d, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}

	testID := "test-id"

	// Expect transaction to begin
	mock.ExpectBegin()

	// Updated mock data with all fields
	row := sqlmock.NewRows([]string{
		"identity_id", "identity_type", "first_name", "last_name", "other_names", "gender", "dob",
		"email_address", "phone_number", "nationality", "organization_name", "category",
		"street", "country", "state", "post_code", "city", "created_at", "meta_data",
	}).AddRow(
		testID, "Individual", "John", "Doe", "Other Names", "Male", time.Now(),
		"john@example.com", "1234567890", "Nationality", "Organization", "Category",
		"Street", "Country", "State", "PostCode", "City", time.Now(), `{"key":"value"}`,
	)

	// Updated query to match the actual method's query
	mock.ExpectQuery("SELECT .* FROM identity WHERE identity_id =").
		WithArgs(testID).
		WillReturnRows(row)

	// Expect transaction to commit
	mock.ExpectCommit()

	result, err := d.GetIdentity(testID)

	// Updated assertions for all fields
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, testID, result.IdentityID)
	assert.Equal(t, "Individual", result.IdentityType)
	assert.Equal(t, "John", result.FirstName)
	// ... continue with assertions for all fields ...

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestGetAllIdentities(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	d, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}

	rows := sqlmock.NewRows([]string{
		"identity_id", "identity_type", "first_name", "last_name", "other_names", "gender", "dob",
		"email_address", "phone_number", "nationality", "organization_name", "category",
		"street", "country", "state", "post_code", "city", "created_at", "meta_data",
	}).AddRow(
		"idt_12345", "individual", "John", "Doe", "Other Names", "Male", time.Now(),
		"john@example.com", "1234567890", "Nationality", "Organization", "Category",
		"Street", "Country", "State", "PostCode", "City", time.Now(), `{"key":"value"}`,
	).AddRow(
		"idt_4442345", "individual", "John", "Doe", "Other Names", "Male", time.Now(),
		"john@example.com", "1234567890", "Nationality", "Organization", "Category",
		"Street", "Country", "State", "PostCode", "City", time.Now(), `{"key":"value"}`,
	)

	mock.ExpectQuery("SELECT .* FROM identity").WillReturnRows(rows)

	result, err := d.GetAllIdentities()

	assert.NoError(t, err)
	assert.Len(t, result, 2)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestUpdateIdentity(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	d, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}

	identity := &model.Identity{
		IdentityType:     "individual",
		OrganizationName: "",
		Category:         "",
		FirstName:        gofakeit.FirstName(),
		LastName:         gofakeit.LastName(),
		OtherNames:       gofakeit.LastName(),
		Gender:           gofakeit.Gender(),
		DOB:              gofakeit.Date(),
		EmailAddress:     gofakeit.Email(),
		PhoneNumber:      gofakeit.Phone(),
		Nationality:      gofakeit.Country(),
		Street:           gofakeit.Street(),
		Country:          gofakeit.Country(),
		State:            gofakeit.State(),
		PostCode:         "0000",
		City:             gofakeit.City(),
		MetaData:         nil,
	}
	metaDataJSON, _ := json.Marshal(identity.MetaData)

	mock.ExpectExec("UPDATE identity SET").
		WithArgs(sqlmock.AnyArg(), identity.IdentityType, identity.FirstName, identity.LastName, identity.OtherNames, identity.Gender, identity.DOB, identity.EmailAddress, identity.PhoneNumber, identity.Nationality, identity.OrganizationName, identity.Category, identity.Street, identity.Country, identity.State, identity.PostCode, identity.City, sqlmock.AnyArg(), metaDataJSON).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = d.UpdateIdentity(identity)

	assert.NoError(t, err)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestDeleteIdentity(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	d, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}

	testID := "idt_123"

	mock.ExpectExec("DELETE FROM identity WHERE identity_id =").
		WithArgs(testID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = d.DeleteIdentity(testID)

	assert.NoError(t, err)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
