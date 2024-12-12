using System;
using System.Linq;
using System.Reflection;

namespace Streamiz.Kafka.Net.Tests.Helpers;

public static class ReflectionHelperExtensionMethods
{
    public static void SetPrivatePropertyValue<T, V>(this T member, string propName, V newValue)
    {
        PropertyInfo[] propertiesInfo =
            typeof(T).GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        var propertyInfo = propertiesInfo.FirstOrDefault(p => p.Name.Equals(propName));
        
        if (propertyInfo == null) return;
            propertyInfo.SetValue(member, newValue);
    }
    
    public static void SetPrivateFieldsValue<V>(this object member, Type type, string propName, V newValue)
    {
        FieldInfo[] fieldsInfo =
            type.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        var fieldInfo = fieldsInfo.FirstOrDefault(p => p.Name.Equals(propName));
        
        if (fieldInfo == null) return;
        fieldInfo.SetValue(member, newValue);
    }
    
    public static object GetInstance(string strFullyQualifiedName, ref Type outputType)
    {
        Type type = Type.GetType(strFullyQualifiedName);
        if (type != null)
        {
            outputType = type;
            return Activator.CreateInstance(type);
        }

        foreach (var asm in AppDomain.CurrentDomain.GetAssemblies())
        {
            type = asm.GetType(strFullyQualifiedName);
            if (type != null)
            {
                outputType = type;
                return Activator.CreateInstance(type);
            }
        }
        return null;
    }
}