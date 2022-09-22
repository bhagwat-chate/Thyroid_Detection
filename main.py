from training_validation_insertion import train_validation

if __name__ == '__main__':
    dbName = 'test'
    tableName = 'Thyroid'
    obj = train_validation(dbName, tableName)
    obj.train_validation()

    print("DONE")
