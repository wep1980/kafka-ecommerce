package br.com.wepdev;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;


public class GsonDeserialized<T> implements Deserializer<T> {

    public static final String TYPE_CONFIG = "br.com.wepdev.ecommerce.type_config";
    private final Gson gson = new GsonBuilder().create();
    private Class<T> type;


    /**
     * Metodo que recebe as configuraçõs do properties do kafka
     * @param configs
     * @param isKey
     */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
       String typeName = String.valueOf(configs.get(TYPE_CONFIG));
        try {
             this.type = (Class<T>) Class.forName(typeName); // Transformando um tipo em uma classe
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Esse tipo de classe não existe no classpath", e);
        }
    }

    /**
     * new String(bytes) -> Transformando os bytes em String
     * @param s
     * @param bytes
     * @return
     */
    @Override
    public T deserialize(String s, byte[] bytes) {
        return gson.fromJson(new String(bytes), type); // deseralizando os bytes no tipo informado
    }
}
