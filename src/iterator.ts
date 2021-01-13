export class AurumDBIterator<T> {
    private iterator: any;
    public current: { key: string; value: T };

    constructor(iterator: any) {
        this.iterator = iterator;
    }

    public next(): Promise<{ key: string; value: T }> {
        return new Promise<{ key: string; value: T }>((resolve, reject) => {
            this.iterator.next((err, key, value) => {
                if (err) {
                    this.end();
                    reject(err);
                } else {
                    if (!key) {
                        this.end();
                    }
                    if (!key) {
                        this.current = undefined;
                    } else {
                        this.current = { key, value };
                    }
                    resolve(this.current);
                }
            });
        });
    }

    public async end(): Promise<void> {
        return new Promise<void>((resolve, reject) => {
            this.current = undefined;
            this.iterator.end((err) => {
                if (err) {
                    reject(err);
                } else {
                    resolve();
                }
            });
        });
    }
}
